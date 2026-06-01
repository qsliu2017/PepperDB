//! re_*comp and friends - compile REs
//!
//! Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
//!
//! Development of this software was funded, in part, by Cray Research Inc.,
//! UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
//! Corporation, none of whom are responsible for the results.  The author
//! thanks all of them.
//!
//! Translated 1:1 from postgres/src/backend/regex/regcomp.c
//!
//! In the C build this file #includes regc_lex.c, regc_color.c, regc_nfa.c,
//! regc_cvec.c, regc_pg_locale.c and regc_locale.c.  In the Rust port those
//! become sibling modules; their `static` functions are referenced here as
//! cross-module dependencies (stubbed with TODO(pg-port) bodies where not yet
//! translated).

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(unused_assignments)]

use std::ffi::{c_char, c_int, c_long, c_void};

use crate::postgres_ext::Oid;
use crate::regex::regcustom::chr;
use crate::regex::regerror::{
    REG_ASSERT, REG_BADBR, REG_BADRPT, REG_ECOLLATE, REG_ECTYPE, REG_EPAREN, REG_ERANGE,
    REG_ESPACE, REG_ESUBREG, REG_ETOOBIG, REG_INVARG,
};
use crate::regex::regex::{
    regex_t, REG_ADVANCED, REG_ADVF, REG_EXPANDED, REG_EXTENDED, REG_ICASE, REG_NEWLINE,
    REG_NLANCH, REG_NLSTOP, REG_NOSUB, REG_QUOTE, REG_ULOCALE, REG_UPBOTCH, REG_USHORTEST,
    REG_UUNPORT, REG_UUNSPEC,
};
use crate::regex::regguts::{
    arc, char_classes, cnfa, color, colordesc, colormap, cvec, fns, guts, nfa, state, subre,
    CC_WORD, COLORLESS, DUPINF, DUPMAX, GUTSMAGIC, NULLCNFA, NUM_CCLASSES, RAINBOW, REMAGIC,
    STACK_TOO_DEEP, UNUSEDCOLOR, ZAPCNFA, BACKR, BRUSE, CAP, COLMARK, INUSE, LATYPE_AHEAD_NEG,
    LATYPE_AHEAD_POS, LATYPE_BEHIND_NEG, LATYPE_BEHIND_POS, LATYPE_IS_AHEAD, LONGER, MATCHALL,
    MESSY, MIXED, NOTREACHED, PREF, PSEUDO, SHORTER, COMBINE, UP, CDEND,
};
use crate::utils::misc::stack_depth::stack_is_too_deep;
use crate::utils::palloc::{palloc_extended, pfree, MCXT_ALLOC_NO_OOM};

use crate::Assert;

// ---------------------------------------------------------------------------
// regcustom.h allocator macros, expressed at the use sites in C.
// ---------------------------------------------------------------------------

/// C: #define MALLOC(n) palloc_extended((n), MCXT_ALLOC_NO_OOM)
unsafe fn MALLOC(n: usize) -> *mut c_void {
    palloc_extended(n, MCXT_ALLOC_NO_OOM)
}

/// C: #define FREE(p) pfree(VS(p))
unsafe fn FREE(p: *mut c_void) {
    pfree(p);
}

/// C: #define REALLOC(p,n) repalloc_extended(VS(p),(n), MCXT_ALLOC_NO_OOM)
unsafe fn REALLOC(p: *mut c_void, n: usize) -> *mut c_void {
    crate::utils::mmgr::mcxt::repalloc_extended(p, n, MCXT_ALLOC_NO_OOM)
}

// ---------------------------------------------------------------------------
// token type codes, some also used as NFA arc types
// ---------------------------------------------------------------------------

pub const EMPTY: c_int = b'n' as c_int; // no token present
pub const EOS: c_int = b'e' as c_int; // end of string
pub const PLAIN: c_int = b'p' as c_int; // ordinary character
pub const DIGIT: c_int = b'd' as c_int; // digit (in bound)
pub const BACKREF: c_int = b'b' as c_int; // back reference
pub const COLLEL: c_int = b'I' as c_int; // start of [.
pub const ECLASS: c_int = b'E' as c_int; // start of [=
pub const CCLASS: c_int = b'C' as c_int; // start of [:
pub const END: c_int = b'X' as c_int; // end of [. [= [:
pub const CCLASSS: c_int = b's' as c_int; // char class shorthand escape
pub const CCLASSC: c_int = b'c' as c_int; // complement char class shorthand escape
pub const RANGE: c_int = b'R' as c_int; // - within [] which might be range delim.
pub const LACON: c_int = b'L' as c_int; // lookaround constraint subRE
pub const AHEAD: c_int = b'a' as c_int; // color-lookahead arc
pub const BEHIND: c_int = b'r' as c_int; // color-lookbehind arc
pub const WBDRY: c_int = b'w' as c_int; // word boundary constraint
pub const NWBDRY: c_int = b'W' as c_int; // non-word-boundary constraint
pub const CANTMATCH: c_int = b'x' as c_int; // arc that cannot match anything
pub const SBEGIN: c_int = b'A' as c_int; // beginning of string (even if not BOL)
pub const SEND: c_int = b'Z' as c_int; // end of string (even if not EOL)

// constants used by combine() in regc_nfa.c (defined in regcomp.c)
pub const INCOMPATIBLE: c_int = 1; // destroys arc
pub const SATISFIED: c_int = 2; // constraint satisfied
pub const COMPATIBLE: c_int = 3; // compatible but not satisfied yet
pub const REPLACEARC: c_int = 4; // replace arc's color with constraint color

/// is an arc colored, and hence should belong to a color chain?
/// the test on "co" eliminates RAINBOW arcs, which we don't bother to chain
/// #define COLORED(a) ((a)->co >= 0 && ((a)->type == PLAIN || ...))
#[inline]
pub unsafe fn COLORED(a: *const arc) -> bool {
    (*a).co >= 0
        && ((*a).r#type == PLAIN || (*a).r#type == AHEAD || (*a).r#type == BEHIND)
}

// ---------------------------------------------------------------------------
// internal variables, bundled for easy passing around
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct vars {
    pub re: *mut regex_t,
    pub now: *const chr,        // scan pointer into string
    pub stop: *const chr,       // end of string
    pub err: c_int,             // error code (0 if none)
    pub cflags: c_int,          // copy of compile flags
    pub lasttype: c_int,        // type of previous token
    pub nexttype: c_int,        // type of next token
    pub nextvalue: chr,         // value (if any) of next token
    pub lexcon: c_int,          // lexical context type (see regc_lex.c)
    pub nsubexp: c_int,         // subexpression count
    pub subs: *mut *mut subre,  // subRE pointer vector
    pub nsubs: usize,           // length of vector
    pub sub10: [*mut subre; 10], // initial vector, enough for most
    pub nfa: *mut nfa,          // the NFA
    pub cm: *mut colormap,      // character color map
    pub nlcolor: color,         // color of newline
    pub wordchrs: *mut state,   // state in nfa holding word-char outarcs
    pub tree: *mut subre,       // subexpression tree
    pub treechain: *mut subre,  // all tree nodes allocated
    pub treefree: *mut subre,   // any free tree nodes
    pub ntree: c_int,           // number of tree nodes, plus one
    pub cv: *mut cvec,          // interface cvec
    pub cv2: *mut cvec,         // utility cvec
    pub lacons: *mut subre,     // lookaround-constraint vector
    pub nlacons: c_int,         // size of lacons[]; only slots 1 .. nlacons-1 used
    pub spaceused: usize,       // approx. space used for compilation
}

// ---------------------------------------------------------------------------
// parsing macros; most know that `v' is the struct vars pointer
// ---------------------------------------------------------------------------

/// #define NEXT() (next(v)) -- advance by one token
macro_rules! NEXT {
    ($v:expr) => {
        next($v)
    };
}

/// #define SEE(t) (v->nexttype == (t)) -- is next token this?
macro_rules! SEE {
    ($v:expr, $t:expr) => {
        (*$v).nexttype == ($t)
    };
}

/// #define EAT(t) (SEE(t) && next(v)) -- if next is this, swallow it
macro_rules! EAT {
    ($v:expr, $t:expr) => {
        (SEE!($v, $t) && next($v) != 0)
    };
}

/// #define VISERR(vv) ((vv)->err != 0) -- have we seen an error yet?
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

/// #define NOERRN() {if (ISERR()) return NULL;}
macro_rules! NOERRN {
    ($v:expr) => {
        if ISERR!($v) {
            return std::ptr::null_mut();
        }
    };
}

/// #define NOERRZ() {if (ISERR()) return 0;}
macro_rules! NOERRZ {
    ($v:expr) => {
        if ISERR!($v) {
            return 0;
        }
    };
}

/// #define INSIST(c, e) do { if (!(c)) ERR(e); } while (0)
macro_rules! INSIST {
    ($v:expr, $c:expr, $e:expr) => {
        if !($c) {
            ERR!($v, $e);
        }
    };
}

/// #define NOTE(b) (v->re->re_info |= (b)) -- note visible condition
macro_rules! NOTE {
    ($v:expr, $b:expr) => {
        (*(*$v).re).re_info |= ($b)
    };
}

/// #define EMPTYARC(x, y) newarc(v->nfa, EMPTY, 0, x, y)
macro_rules! EMPTYARC {
    ($v:expr, $x:expr, $y:expr) => {
        newarc((*$v).nfa, EMPTY, 0, $x, $y)
    };
}

// ---------------------------------------------------------------------------
// static function list
// ---------------------------------------------------------------------------

static functions: fns = fns {
    free: Some(rfree_thunk),    // regfree insides
    stack_too_deep: Some(rstacktoodeep), // check for stack getting dangerously deep
};

// rfree() takes the real regex_t; the fns table types its argument as the void
// regex_t.  Wrap rfree at the C-ABI boundary, mirroring how regfree.rs casts.
unsafe extern "C" fn rfree_thunk(re: *mut c_void) {
    rfree(re as *mut regex_t);
}

/*
 * pg_regcomp - compile regular expression
 *
 * Note: on failure, no resources remain allocated, so pg_regfree()
 * need not be applied to re.
 */
pub unsafe fn pg_regcomp(
    re: *mut regex_t,
    string: *const chr,
    len: usize,
    flags: c_int,
    collation: Oid,
) -> c_int {
    let mut var: vars = std::mem::zeroed();
    let v: *mut vars = &mut var;
    let g: *mut guts;
    let mut i: c_int;
    let mut j: usize;

    // #ifdef REG_DEBUG ... #else
    let debug: *mut c_void = std::ptr::null_mut();
    let _ = debug;

    // #define CNOERR() { if (ISERR()) return freev(v, v->err); }
    macro_rules! CNOERR {
        () => {
            if ISERR!(v) {
                return freev(v, (*v).err);
            }
        };
    }

    /* sanity checks */

    if re.is_null() || string.is_null() {
        return REG_INVARG;
    }
    if (flags & REG_QUOTE) != 0
        && (flags & (REG_ADVANCED | REG_EXPANDED | REG_NEWLINE)) != 0
    {
        return REG_INVARG;
    }
    if (flags & REG_EXTENDED) == 0 && (flags & REG_ADVF) != 0 {
        return REG_INVARG;
    }

    /* Initialize locale-dependent support */
    pg_set_regex_collation(collation);

    /* initial setup (after which freev() is callable) */
    (*v).re = re;
    (*v).now = string;
    (*v).stop = (*v).now.add(len);
    (*v).err = 0;
    (*v).cflags = flags;
    (*v).nsubexp = 0;
    (*v).subs = (*v).sub10.as_mut_ptr();
    (*v).nsubs = 10;
    j = 0;
    while j < (*v).nsubs {
        *(*v).subs.add(j) = std::ptr::null_mut();
        j += 1;
    }
    (*v).nfa = std::ptr::null_mut();
    (*v).cm = std::ptr::null_mut();
    (*v).nlcolor = COLORLESS;
    (*v).wordchrs = std::ptr::null_mut();
    (*v).tree = std::ptr::null_mut();
    (*v).treechain = std::ptr::null_mut();
    (*v).treefree = std::ptr::null_mut();
    (*v).cv = std::ptr::null_mut();
    (*v).cv2 = std::ptr::null_mut();
    (*v).lacons = std::ptr::null_mut();
    (*v).nlacons = 0;
    (*v).spaceused = 0;
    (*re).re_magic = REMAGIC;
    (*re).re_info = 0; /* bits get set during parse */
    (*re).re_csize = std::mem::size_of::<chr>() as c_int;
    (*re).re_collation = collation;
    (*re).re_guts = std::ptr::null_mut();
    (*re).re_fns = (&functions as *const fns as *mut fns) as *mut c_char;

    /* more complex setup, malloced things */
    (*re).re_guts = MALLOC(std::mem::size_of::<guts>()) as *mut c_char;
    if (*re).re_guts.is_null() {
        return freev(v, REG_ESPACE);
    }
    g = (*re).re_guts as *mut guts;
    (*g).tree = std::ptr::null_mut();
    initcm(v, &mut (*g).cmap);
    (*v).cm = &mut (*g).cmap;
    (*g).lacons = std::ptr::null_mut();
    (*g).nlacons = 0;
    ZAPCNFA(&mut (*g).search);
    (*v).nfa = newnfa(v, (*v).cm, std::ptr::null_mut());
    CNOERR!();
    /* set up a reasonably-sized transient cvec for getcvec usage */
    (*v).cv = newcvec(100, 20);
    if (*v).cv.is_null() {
        return freev(v, REG_ESPACE);
    }

    /* parsing */
    lexstart(v); /* also handles prefixes */
    if ((*v).cflags & REG_NLSTOP) != 0 || ((*v).cflags & REG_NLANCH) != 0 {
        /* assign newline a unique color */
        (*v).nlcolor = subcolor((*v).cm, newline());
        okcolors((*v).nfa, (*v).cm);
    }
    CNOERR!();
    (*v).tree = parse(v, EOS, PLAIN, (*(*v).nfa).init, (*(*v).nfa).r#final);
    Assert!(SEE!(v, EOS)); /* even if error; ISERR() => SEE(EOS) */
    CNOERR!();
    Assert!(!(*v).tree.is_null());

    /* finish setup of nfa and its subre tree */
    specialcolors((*v).nfa);
    CNOERR!();

    if (*v).cflags & REG_NOSUB != 0 {
        removecaptures(v, (*v).tree);
    }
    (*v).ntree = numst((*v).tree, 1);
    markst((*v).tree);
    cleanst(v);

    /* build compacted NFAs for tree and lacons */
    (*re).re_info |= nfatree(v, (*v).tree, debug);
    CNOERR!();
    Assert!((*v).nlacons == 0 || !(*v).lacons.is_null());
    i = 1;
    while i < (*v).nlacons {
        let lasub: *mut subre = &mut *(*v).lacons.add(i as usize);

        /* Prepend .* to pattern if it's a lookbehind LACON */
        nfanode(
            v,
            lasub,
            (LATYPE_IS_AHEAD((*lasub).latype as c_int) == 0) as c_int,
            debug,
        );
        i += 1;
    }
    CNOERR!();
    if (*(*v).tree).flags as c_int & SHORTER != 0 {
        NOTE!(v, REG_USHORTEST);
    }

    /* build compacted NFAs for tree, lacons, fast search */
    /* can sacrifice main NFA now, so use it as work area */
    let _: c_long = optimize((*v).nfa, debug); /* (DISCARD) */
    CNOERR!();
    makesearch(v, (*v).nfa);
    CNOERR!();
    compact((*v).nfa, &mut (*g).search);
    CNOERR!();

    /* looks okay, package it up */
    (*re).re_nsub = (*v).nsubexp as usize;
    (*v).re = std::ptr::null_mut(); /* freev no longer frees re */
    (*g).magic = GUTSMAGIC;
    (*g).cflags = (*v).cflags;
    (*g).info = (*re).re_info;
    (*g).nsub = (*re).re_nsub;
    (*g).tree = (*v).tree;
    (*v).tree = std::ptr::null_mut();
    (*g).ntree = (*v).ntree;
    (*g).compare = if (*v).cflags & REG_ICASE != 0 {
        Some(casecmp as unsafe extern "C" fn(*const chr, *const chr, usize) -> c_int)
    } else {
        Some(cmp as unsafe extern "C" fn(*const chr, *const chr, usize) -> c_int)
    };
    (*g).lacons = (*v).lacons;
    (*v).lacons = std::ptr::null_mut();
    (*g).nlacons = (*v).nlacons;

    Assert!((*v).err == 0);
    freev(v, 0)
}

/*
 * moresubs - enlarge subRE vector
 */
unsafe fn moresubs(v: *mut vars, wanted: c_int) {
    let mut p: *mut *mut subre;
    let n: usize;

    Assert!(wanted > 0 && wanted as usize >= (*v).nsubs);
    n = wanted as usize * 3 / 2 + 1;

    if (*v).subs == (*v).sub10.as_mut_ptr() {
        p = MALLOC(n * std::mem::size_of::<*mut subre>()) as *mut *mut subre;
        if !p.is_null() {
            std::ptr::copy_nonoverlapping(
                (*v).subs as *const c_void,
                p as *mut c_void,
                (*v).nsubs * std::mem::size_of::<*mut subre>(),
            );
        }
    } else {
        p = REALLOC(
            (*v).subs as *mut c_void,
            n * std::mem::size_of::<*mut subre>(),
        ) as *mut *mut subre;
    }
    if p.is_null() {
        ERR!(v, REG_ESPACE);
        return;
    }
    (*v).subs = p;
    p = (*v).subs.add((*v).nsubs);
    while (*v).nsubs < n {
        *p = std::ptr::null_mut();
        p = p.add(1);
        (*v).nsubs += 1;
    }
    Assert!((*v).nsubs == n);
    Assert!((wanted as usize) < (*v).nsubs);
}

/*
 * freev - free vars struct's substructures where necessary
 *
 * Optionally does error-number setting, and always returns error code
 * (if any), to make error-handling code terser.
 */
unsafe fn freev(v: *mut vars, err: c_int) -> c_int {
    if !(*v).re.is_null() {
        rfree((*v).re);
    }
    if (*v).subs != (*v).sub10.as_mut_ptr() {
        FREE((*v).subs as *mut c_void);
    }
    if !(*v).nfa.is_null() {
        freenfa((*v).nfa);
    }
    if !(*v).tree.is_null() {
        freesubre(v, (*v).tree);
    }
    if !(*v).treechain.is_null() {
        cleanst(v);
    }
    if !(*v).cv.is_null() {
        freecvec((*v).cv);
    }
    if !(*v).cv2.is_null() {
        freecvec((*v).cv2);
    }
    if !(*v).lacons.is_null() {
        freelacons((*v).lacons, (*v).nlacons);
    }
    ERR!(v, err); /* nop if err==0 */

    (*v).err
}

/*
 * makesearch - turn an NFA into a search NFA (implicit prepend of .*?)
 * NFA must have been optimize()d already.
 */
unsafe fn makesearch(v: *mut vars, nfa: *mut nfa) {
    let mut a: *mut arc;
    let mut b: *mut arc;
    let pre: *mut state = (*nfa).pre;
    let mut s: *mut state;
    let mut s2: *mut state;
    let mut slist: *mut state;

    /* no loops are needed if it's anchored */
    a = (*pre).outs;
    while !a.is_null() {
        Assert!((*a).r#type == PLAIN);
        if (*a).co != (*nfa).bos[0] && (*a).co != (*nfa).bos[1] {
            break;
        }
        a = (*a).outchain;
    }
    if !a.is_null() {
        /* add implicit .* in front */
        rainbow(nfa, (*v).cm, PLAIN, COLORLESS, pre, pre);

        /* and ^* and \A* too -- not always necessary, but harmless */
        newarc(nfa, PLAIN, (*nfa).bos[0], pre, pre);
        newarc(nfa, PLAIN, (*nfa).bos[1], pre, pre);

        /*
         * The pattern is still MATCHALL if it was before, but the max match
         * length is now infinity.
         */
        if (*nfa).flags & MATCHALL != 0 {
            (*nfa).maxmatchall = DUPINF;
        }
    }

    /*
     * Now here's the subtle part.  Because many REs have no lookback
     * constraints, often knowing when you were in the pre state tells you
     * little; it's the next state(s) that are informative.  But some of them
     * may have other inarcs, i.e. it may be possible to make actual progress
     * and then return to one of them.  We must de-optimize such cases,
     * splitting each such state into progress and no-progress states.
     */

    /* first, make a list of the states reachable from pre and elsewhere */
    slist = std::ptr::null_mut();
    a = (*pre).outs;
    while !a.is_null() {
        s = (*a).to;
        b = (*s).ins;
        while !b.is_null() {
            if (*b).from != pre {
                break;
            }
            b = (*b).inchain;
        }

        /*
         * We want to mark states as being in the list already by having non
         * NULL tmp fields, but we can't just store the old slist value in tmp
         * because that doesn't work for the first such state.  Instead, the
         * first list entry gets its own address in tmp.
         */
        if !b.is_null() && (*s).tmp.is_null() {
            (*s).tmp = if !slist.is_null() { slist } else { s };
            slist = s;
        }
        a = (*a).outchain;
    }

    /* do the splits */
    s = slist;
    while !s.is_null() {
        s2 = newstate(nfa);
        NOERR!(v);
        copyouts(nfa, s, s2);
        NOERR!(v);
        a = (*s).ins;
        while !a.is_null() {
            b = (*a).inchain;
            if (*a).from != pre {
                cparc(nfa, a, (*a).from, s2);
                freearc(nfa, a);
            }
            a = b;
        }
        s2 = if (*s).tmp != s { (*s).tmp } else { std::ptr::null_mut() };
        (*s).tmp = std::ptr::null_mut(); /* clean up while we're at it */
        s = s2;
    }
}

/*
 * parse - parse an RE
 *
 * This is actually just the top level, which parses a bunch of branches
 * tied together with '|'.  If there's more than one, they appear in the
 * tree as the children of a '|' subre.
 */
unsafe fn parse(
    v: *mut vars,
    stopper: c_int, /* EOS or ')' */
    r#type: c_int,  /* LACON (lookaround subRE) or PLAIN */
    init: *mut state,
    r#final: *mut state,
) -> *mut subre {
    let mut branches: *mut subre; /* top level */
    let mut lastbranch: *mut subre; /* latest branch */

    Assert!(stopper == ')' as c_int || stopper == EOS);

    branches = subre(v, '|' as c_int, LONGER, init, r#final);
    NOERRN!(v);
    lastbranch = std::ptr::null_mut();
    loop {
        /* a branch */
        let branch: *mut subre;
        let left: *mut state; /* scaffolding for branch */
        let right: *mut state;

        left = newstate((*v).nfa);
        right = newstate((*v).nfa);
        NOERRN!(v);
        EMPTYARC!(v, init, left);
        EMPTYARC!(v, right, r#final);
        NOERRN!(v);
        branch = parsebranch(v, stopper, r#type, left, right, 0);
        NOERRN!(v);
        if !lastbranch.is_null() {
            (*lastbranch).sibling = branch;
        } else {
            (*branches).child = branch;
        }
        (*branches).flags |=
            UP((*branches).flags as c_int | (*branch).flags as c_int) as c_char;
        lastbranch = branch;

        if !EAT!(v, '|' as c_int) {
            break;
        }
    }
    Assert!(SEE!(v, stopper) || SEE!(v, EOS));

    if !SEE!(v, stopper) {
        Assert!(stopper == ')' as c_int && SEE!(v, EOS));
        ERR!(v, REG_EPAREN);
    }

    /* optimize out simple cases */
    if lastbranch == (*branches).child {
        /* only one branch */
        Assert!((*lastbranch).sibling.is_null());
        freesrnode(v, branches);
        branches = lastbranch;
    } else if MESSY((*branches).flags as c_int) == 0 {
        /* no interesting innards */
        freesubreandsiblings(v, (*branches).child);
        (*branches).child = std::ptr::null_mut();
        (*branches).op = '=' as c_char;
    }

    branches
}

/*
 * parsebranch - parse one branch of an RE
 *
 * This mostly manages concatenation, working closely with parseqatom().
 * Concatenated things are bundled up as much as possible, with separate
 * '.' nodes introduced only when necessary due to substructure.
 */
unsafe fn parsebranch(
    v: *mut vars,
    stopper: c_int, /* EOS or ')' */
    r#type: c_int,  /* LACON (lookaround subRE) or PLAIN */
    left: *mut state, /* leftmost state */
    right: *mut state, /* rightmost state */
    partial: c_int, /* is this only part of a branch? */
) -> *mut subre {
    let mut lp: *mut state; /* left end of current construct */
    let mut seencontent: c_int; /* is there anything in this branch yet? */
    let mut t: *mut subre;

    lp = left;
    seencontent = 0;
    t = subre(v, '=' as c_int, 0, left, right); /* op '=' is tentative */
    NOERRN!(v);
    while !SEE!(v, '|' as c_int) && !SEE!(v, stopper) && !SEE!(v, EOS) {
        if seencontent != 0 {
            /* implicit concat operator */
            lp = newstate((*v).nfa);
            NOERRN!(v);
            moveins((*v).nfa, right, lp);
        }
        seencontent = 1;

        /* NB, recursion in parseqatom() may swallow rest of branch */
        t = parseqatom(v, stopper, r#type, lp, right, t);
        NOERRN!(v);
    }

    if seencontent == 0 {
        /* empty branch */
        if partial == 0 {
            NOTE!(v, REG_UUNSPEC);
        }
        Assert!(lp == left);
        EMPTYARC!(v, left, right);
    }

    t
}

/*
 * parseqatom - parse one quantified atom or constraint of an RE
 *
 * The bookkeeping near the end cooperates very closely with parsebranch();
 * in particular, it contains a recursion that can involve parsing the rest
 * of the branch, making this function's name somewhat inaccurate.
 *
 * Usually, the return value is just "top", but in some cases where we
 * have parsed the rest of the branch, we may deem "top" redundant and
 * free it, returning some child subre instead.
 */
unsafe fn parseqatom(
    v: *mut vars,
    stopper: c_int, /* EOS or ')' */
    r#type: c_int,  /* LACON (lookaround subRE) or PLAIN */
    lp: *mut state, /* left state to hang it on */
    rp: *mut state, /* right state to hang it on */
    mut top: *mut subre, /* subtree top */
) -> *mut subre {
    let mut s: *mut state; /* temporaries for new states */
    let mut s2: *mut state;

    // #define ARCV(t, val) newarc(v->nfa, t, val, lp, rp)
    macro_rules! ARCV {
        ($t:expr, $val:expr) => {
            newarc((*v).nfa, $t, $val, lp, rp)
        };
    }

    let mut m: c_int;
    let mut n: c_int;
    let mut atom: *mut subre; /* atom's subtree */
    let mut t: *mut subre;
    let cap: c_int; /* capturing parens? */
    let latype: c_int; /* lookaround constraint type */
    let mut subno: c_int; /* capturing-parens or backref number */
    let mut atomtype: c_int;
    let qprefer: c_int; /* quantifier short/long preference */
    let mut f: c_int;
    let atomp: *mut *mut subre; /* where the pointer to atom is */

    /* initial bookkeeping */
    atom = std::ptr::null_mut();
    Assert!((*lp).nouts == 0); /* must string new code */
    Assert!((*rp).nins == 0); /* between lp and rp */
    subno = 0; /* just to shut lint up */

    /* an atom or constraint... */
    atomtype = (*v).nexttype;
    match atomtype {
        /* first, constraints, which end by returning */
        x if x == '^' as c_int => {
            ARCV!('^' as c_int, 1);
            if (*v).cflags & REG_NLANCH != 0 {
                ARCV!(BEHIND, (*v).nlcolor);
            }
            NEXT!(v);
            return top;
        }
        x if x == '$' as c_int => {
            ARCV!('$' as c_int, 1);
            if (*v).cflags & REG_NLANCH != 0 {
                ARCV!(AHEAD, (*v).nlcolor);
            }
            NEXT!(v);
            return top;
        }
        x if x == SBEGIN => {
            ARCV!('^' as c_int, 1); /* BOL */
            ARCV!('^' as c_int, 0); /* or BOS */
            NEXT!(v);
            return top;
        }
        x if x == SEND => {
            ARCV!('$' as c_int, 1); /* EOL */
            ARCV!('$' as c_int, 0); /* or EOS */
            NEXT!(v);
            return top;
        }
        x if x == '<' as c_int => {
            wordchrs(v);
            s = newstate((*v).nfa);
            NOERRN!(v);
            nonword(v, BEHIND, lp, s);
            word(v, AHEAD, s, rp);
            NEXT!(v);
            return top;
        }
        x if x == '>' as c_int => {
            wordchrs(v);
            s = newstate((*v).nfa);
            NOERRN!(v);
            word(v, BEHIND, lp, s);
            nonword(v, AHEAD, s, rp);
            NEXT!(v);
            return top;
        }
        x if x == WBDRY => {
            wordchrs(v);
            s = newstate((*v).nfa);
            NOERRN!(v);
            nonword(v, BEHIND, lp, s);
            word(v, AHEAD, s, rp);
            s = newstate((*v).nfa);
            NOERRN!(v);
            word(v, BEHIND, lp, s);
            nonword(v, AHEAD, s, rp);
            NEXT!(v);
            return top;
        }
        x if x == NWBDRY => {
            wordchrs(v);
            s = newstate((*v).nfa);
            NOERRN!(v);
            word(v, BEHIND, lp, s);
            word(v, AHEAD, s, rp);
            s = newstate((*v).nfa);
            NOERRN!(v);
            nonword(v, BEHIND, lp, s);
            nonword(v, AHEAD, s, rp);
            NEXT!(v);
            return top;
        }
        x if x == LACON => {
            /* lookaround constraint */
            latype = (*v).nextvalue as c_int;
            NEXT!(v);
            s = newstate((*v).nfa);
            s2 = newstate((*v).nfa);
            NOERRN!(v);
            t = parse(v, ')' as c_int, LACON, s, s2);
            freesubre(v, t); /* internal structure irrelevant */
            NOERRN!(v);
            Assert!(SEE!(v, ')' as c_int));
            NEXT!(v);
            processlacon(v, s, s2, latype, lp, rp);
            return top;
        }
        /* then errors, to get them out of the way */
        x if x == '*' as c_int
            || x == '+' as c_int
            || x == '?' as c_int
            || x == '{' as c_int =>
        {
            ERR!(v, REG_BADRPT);
            return top;
        }
        x if x == ')' as c_int
            || x == PLAIN
            || x == '[' as c_int
            || x == CCLASSS
            || x == CCLASSC
            || x == '.' as c_int
            || x == '(' as c_int
            || x == BACKREF =>
        {
            /* then plain characters, and minor variants on that theme */
            // handled below; fall out of the match into a second switch
            'plainstuff: {
                if atomtype == ')' as c_int {
                    /* unbalanced paren */
                    if ((*v).cflags & REG_ADVANCED) != REG_EXTENDED {
                        ERR!(v, REG_EPAREN);
                        return top;
                    }
                    /* legal in EREs due to specification botch */
                    NOTE!(v, REG_UPBOTCH);
                    /* fall through into case PLAIN */
                    onechr(v, (*v).nextvalue, lp, rp);
                    okcolors((*v).nfa, (*v).cm);
                    NOERRN!(v);
                    NEXT!(v);
                    break 'plainstuff;
                }
                if atomtype == PLAIN {
                    onechr(v, (*v).nextvalue, lp, rp);
                    okcolors((*v).nfa, (*v).cm);
                    NOERRN!(v);
                    NEXT!(v);
                    break 'plainstuff;
                }
                if atomtype == '[' as c_int {
                    if (*v).nextvalue == 1 {
                        bracket(v, lp, rp);
                    } else {
                        cbracket(v, lp, rp);
                    }
                    Assert!(SEE!(v, ']' as c_int) || ISERR!(v));
                    NEXT!(v);
                    break 'plainstuff;
                }
                if atomtype == CCLASSS {
                    charclass(v, (*v).nextvalue as char_classes, lp, rp);
                    okcolors((*v).nfa, (*v).cm);
                    NEXT!(v);
                    break 'plainstuff;
                }
                if atomtype == CCLASSC {
                    charclasscomplement(v, (*v).nextvalue as char_classes, lp, rp);
                    /* charclasscomplement() did okcolors() internally */
                    NEXT!(v);
                    break 'plainstuff;
                }
                if atomtype == '.' as c_int {
                    rainbow(
                        (*v).nfa,
                        (*v).cm,
                        PLAIN,
                        if (*v).cflags & REG_NLSTOP != 0 {
                            (*v).nlcolor
                        } else {
                            COLORLESS
                        },
                        lp,
                        rp,
                    );
                    NEXT!(v);
                    break 'plainstuff;
                }
                /* and finally the ugly stuff */
                if atomtype == '(' as c_int {
                    /* value flags as capturing or non */
                    cap = if r#type == LACON { 0 } else { (*v).nextvalue as c_int };
                    if cap != 0 {
                        (*v).nsubexp += 1;
                        subno = (*v).nsubexp;
                        if subno as usize >= (*v).nsubs {
                            moresubs(v, subno);
                        }
                    } else {
                        atomtype = PLAIN; /* something that's not '(' */
                    }
                    NEXT!(v);

                    /*
                     * Make separate endpoint states to keep this sub-NFA
                     * distinct from what surrounds it.  We need to be sure that
                     * when we duplicate the sub-NFA for a backref, we get the
                     * right states/arcs and no others.  In particular, letting a
                     * backref duplicate the sub-NFA from lp to rp would be quite
                     * wrong, because we may add quantification superstructure
                     * around this atom below.  (Perhaps we could skip the extra
                     * states for non-capturing parens, but it seems not worth the
                     * trouble.)
                     */
                    s = newstate((*v).nfa);
                    s2 = newstate((*v).nfa);
                    NOERRN!(v);
                    /* We may not need these arcs, but keep things connected */
                    EMPTYARC!(v, lp, s);
                    EMPTYARC!(v, s2, rp);
                    NOERRN!(v);
                    atom = parse(v, ')' as c_int, r#type, s, s2);
                    Assert!(SEE!(v, ')' as c_int) || ISERR!(v));
                    NEXT!(v);
                    NOERRN!(v);
                    if cap != 0 {
                        if (*atom).capno == 0 {
                            /* normal case: just mark the atom as capturing */
                            (*atom).flags |= CAP as c_char;
                            (*atom).capno = subno;
                        } else {
                            /* generate no-op wrapper node to handle "((x))" */
                            t = subre(
                                v,
                                '(' as c_int,
                                (*atom).flags as c_int | CAP,
                                s,
                                s2,
                            );
                            NOERRN!(v);
                            (*t).capno = subno;
                            (*t).child = atom;
                            atom = t;
                        }
                        Assert!((*(*v).subs.add(subno as usize)).is_null());
                        *(*v).subs.add(subno as usize) = atom;
                    }
                    /* postpone everything else pending possible {0} */
                    break 'plainstuff;
                }
                if atomtype == BACKREF {
                    /* the Feature From The Black Lagoon */
                    INSIST!(v, r#type != LACON, REG_ESUBREG);
                    subno = (*v).nextvalue as c_int;
                    Assert!(subno > 0);
                    INSIST!(v, (subno as usize) < (*v).nsubs, REG_ESUBREG);
                    NOERRN!(v);
                    INSIST!(v, !(*(*v).subs.add(subno as usize)).is_null(), REG_ESUBREG);
                    NOERRN!(v);
                    atom = subre(v, 'b' as c_int, BACKR, lp, rp);
                    NOERRN!(v);
                    (*atom).backno = subno;
                    (*(*(*v).subs.add(subno as usize))).flags |= BRUSE as c_char;
                    EMPTYARC!(v, lp, rp); /* temporarily, so there's something */
                    NEXT!(v);
                    break 'plainstuff;
                }
            }
        }
        _ => {
            ERR!(v, REG_ASSERT);
            return top;
        }
    }

    /* ...and an atom may be followed by a quantifier */
    match (*v).nexttype {
        x if x == '*' as c_int => {
            m = 0;
            n = DUPINF;
            qprefer = if (*v).nextvalue != 0 { LONGER } else { SHORTER };
            NEXT!(v);
        }
        x if x == '+' as c_int => {
            m = 1;
            n = DUPINF;
            qprefer = if (*v).nextvalue != 0 { LONGER } else { SHORTER };
            NEXT!(v);
        }
        x if x == '?' as c_int => {
            m = 0;
            n = 1;
            qprefer = if (*v).nextvalue != 0 { LONGER } else { SHORTER };
            NEXT!(v);
        }
        x if x == '{' as c_int => {
            NEXT!(v);
            m = scannum(v);
            if EAT!(v, ',' as c_int) {
                if SEE!(v, DIGIT) {
                    n = scannum(v);
                } else {
                    n = DUPINF;
                }
                if m > n {
                    ERR!(v, REG_BADBR);
                    return top;
                }
                /* {m,n} exercises preference, even if it's {m,m} */
                qprefer = if (*v).nextvalue != 0 { LONGER } else { SHORTER };
            } else {
                n = m;
                /* {m} passes operand's preference through */
                qprefer = 0;
            }
            if !SEE!(v, '}' as c_int) {
                /* catches errors too */
                ERR!(v, REG_BADBR);
                return top;
            }
            NEXT!(v);
        }
        _ => {
            /* no quantifier */
            m = 1;
            n = 1;
            qprefer = 0;
        }
    }

    /* annoying special case:  {0} or {0,0} cancels everything */
    if m == 0 && n == 0 {
        /*
         * If we had capturing subexpression(s) within the atom, we don't want
         * to destroy them, because it's legal (if useless) to back-ref them
         * later.  Hence, just unlink the atom from lp/rp and then ignore it.
         */
        if !atom.is_null() && (*atom).flags as c_int & CAP != 0 {
            delsub((*v).nfa, lp, (*atom).begin);
            delsub((*v).nfa, (*atom).end, rp);
        } else {
            /* Otherwise, we can clean up any subre infrastructure we made */
            if !atom.is_null() {
                freesubre(v, atom);
            }
            delsub((*v).nfa, lp, rp);
        }
        EMPTYARC!(v, lp, rp);
        return top;
    }

    /* if not a messy case, avoid hard part */
    Assert!(MESSY((*top).flags as c_int) == 0);
    f = (*top).flags as c_int
        | qprefer
        | (if !atom.is_null() { (*atom).flags as c_int } else { 0 });
    if atomtype != '(' as c_int && atomtype != BACKREF && MESSY(UP(f)) == 0 {
        if !(m == 1 && n == 1) {
            repeat(v, lp, rp, m, n);
        }
        if !atom.is_null() {
            freesubre(v, atom);
        }
        (*top).flags = f as c_char;
        return top;
    }

    /*
     * hard part:  something messy
     *
     * That is, capturing parens, back reference, short/long clash, or an atom
     * with substructure containing one of those.
     */

    /* now we'll need a subre for the contents even if they're boring */
    if atom.is_null() {
        atom = subre(v, '=' as c_int, 0, lp, rp);
        NOERRN!(v);
    }

    /*
     * For what follows, we need the atom to have its own begin/end states
     * that are distinct from lp/rp, so that we can wrap iteration structure
     * around it.  The parenthesized-atom case above already made suitable
     * states (and we don't want to modify a capturing subre, since it's
     * already recorded in v->subs[]).  Otherwise, we need more states.
     */
    if (*atom).begin == lp || (*atom).end == rp {
        s = newstate((*v).nfa);
        s2 = newstate((*v).nfa);
        NOERRN!(v);
        moveouts((*v).nfa, lp, s);
        moveins((*v).nfa, rp, s2);
        (*atom).begin = s;
        (*atom).end = s2;
    } else {
        /* The atom's OK, but we must temporarily disconnect it from lp/rp */
        /* (this removes the EMPTY arcs we made above) */
        delsub((*v).nfa, lp, (*atom).begin);
        delsub((*v).nfa, (*atom).end, rp);
    }

    /*----------
     * Prepare a general-purpose state skeleton.
     *
     * In the no-backrefs case, we want this:
     *
     * [lp] ---> [s] ---prefix---> ---atom---> ---rest---> [rp]
     *
     * where prefix is some repetitions of atom, and "rest" is the remainder
     * of the branch.  In the general case we need:
     *
     * [lp] ---> [s] ---iterator---> [s2] ---rest---> [rp]
     *
     * where the iterator wraps around the atom.
     *
     * We make the s state here for both cases; s2 is made below if needed
     *----------
     */
    s = newstate((*v).nfa); /* set up starting state */
    NOERRN!(v);
    EMPTYARC!(v, lp, s);
    NOERRN!(v);

    /* break remaining subRE into x{...} and what follows */
    t = subre(v, '.' as c_int, COMBINE(qprefer, (*atom).flags as c_int), lp, rp);
    NOERRN!(v);
    (*t).child = atom;
    atomp = &mut (*t).child;

    /*
     * Here we should recurse to fill t->child->sibling ... but we must
     * postpone that to the end.  One reason is that t->child may be replaced
     * below, and we don't want to worry about its sibling link.
     */

    /*
     * Convert top node to a concatenation of the prefix (top->child, covering
     * whatever we parsed previously) and remaining (t).  Note that the prefix
     * could be empty, in which case this concatenation node is unnecessary.
     * To keep things simple, we operate in a general way for now, and get rid
     * of unnecessary subres below.
     */
    Assert!((*top).op == '=' as c_char && (*top).child.is_null());
    (*top).child = subre(v, '=' as c_int, (*top).flags as c_int, (*top).begin, lp);
    NOERRN!(v);
    (*top).op = '.' as c_char;
    (*(*top).child).sibling = t;
    /* top->flags will get updated later */

    /* if it's a backref, now is the time to replicate the subNFA */
    if atomtype == BACKREF {
        Assert!((*(*atom).begin).nouts == 1); /* just the EMPTY */
        delsub((*v).nfa, (*atom).begin, (*atom).end);
        Assert!(!(*(*v).subs.add(subno as usize)).is_null());

        /*
         * And here's why the recursion got postponed: it must wait until the
         * skeleton is filled in, because it may hit a backref that wants to
         * copy the filled-in skeleton.
         */
        dupnfa(
            (*v).nfa,
            (*(*(*v).subs.add(subno as usize))).begin,
            (*(*(*v).subs.add(subno as usize))).end,
            (*atom).begin,
            (*atom).end,
        );
        NOERRN!(v);

        /* The backref node's NFA should not enforce any constraints */
        removeconstraints((*v).nfa, (*atom).begin, (*atom).end);
        NOERRN!(v);
    }

    /*
     * It's quantifier time.  If the atom is just a backref, we'll let it deal
     * with quantifiers internally.
     */
    if atomtype == BACKREF {
        /* special case:  backrefs have internal quantifiers */
        EMPTYARC!(v, s, (*atom).begin); /* empty prefix */
        /* just stuff everything into atom */
        repeat(v, (*atom).begin, (*atom).end, m, n);
        (*atom).min = m as i16;
        (*atom).max = n as i16;
        (*atom).flags |= COMBINE(qprefer, (*atom).flags as c_int) as c_char;
        /* rest of branch can be strung starting from atom->end */
        s2 = (*atom).end;
    } else if m == 1
        && n == 1
        && (qprefer == 0
            || ((*atom).flags as c_int & (LONGER | SHORTER | MIXED)) == 0
            || qprefer == ((*atom).flags as c_int & (LONGER | SHORTER | MIXED)))
    {
        /* no/vacuous quantifier:  done */
        EMPTYARC!(v, s, (*atom).begin); /* empty prefix */
        /* rest of branch can be strung starting from atom->end */
        s2 = (*atom).end;
    } else if (*atom).flags as c_int & (CAP | BACKR) == 0 {
        /*
         * If there's no captures nor backrefs in the atom being repeated, we
         * don't really care where the submatches of the iteration are, so we
         * don't need an iteration node.  Make a plain DFA node instead.
         */
        EMPTYARC!(v, s, (*atom).begin); /* empty prefix */
        repeat(v, (*atom).begin, (*atom).end, m, n);
        f = COMBINE(qprefer, (*atom).flags as c_int);
        t = subre(v, '=' as c_int, f, (*atom).begin, (*atom).end);
        NOERRN!(v);
        freesubre(v, atom);
        *atomp = t;
        /* rest of branch can be strung starting from t->end */
        s2 = (*t).end;
    } else if m > 0 && (*atom).flags as c_int & BACKR == 0 {
        /*
         * If there's no backrefs involved, we can turn x{m,n} into
         * x{m-1,n-1}x, with capturing parens in only the second x.  This is
         * valid because we only care about capturing matches from the final
         * iteration of the quantifier.  It's a win because we can implement
         * the backref-free left side as a plain DFA node, since we don't
         * really care where its submatches are.
         */
        dupnfa((*v).nfa, (*atom).begin, (*atom).end, s, (*atom).begin);
        Assert!(m >= 1 && m != DUPINF && n >= 1);
        repeat(v, s, (*atom).begin, m - 1, if n == DUPINF { n } else { n - 1 });
        f = COMBINE(qprefer, (*atom).flags as c_int);
        t = subre(v, '.' as c_int, f, s, (*atom).end); /* prefix and atom */
        NOERRN!(v);
        (*t).child = subre(v, '=' as c_int, PREF(f), s, (*atom).begin);
        NOERRN!(v);
        (*(*t).child).sibling = atom;
        *atomp = t;
        /* rest of branch can be strung starting from atom->end */
        s2 = (*atom).end;
    } else {
        /* general case: need an iteration node */
        s2 = newstate((*v).nfa);
        NOERRN!(v);
        moveouts((*v).nfa, (*atom).end, s2);
        NOERRN!(v);
        dupnfa((*v).nfa, (*atom).begin, (*atom).end, s, s2);
        repeat(v, s, s2, m, n);
        f = COMBINE(qprefer, (*atom).flags as c_int);
        t = subre(v, '*' as c_int, f, s, s2);
        NOERRN!(v);
        (*t).min = m as i16;
        (*t).max = n as i16;
        (*t).child = atom;
        *atomp = t;
        /* rest of branch is to be strung from iteration's end state */
    }

    /* and finally, look after that postponed recursion */
    t = (*(*top).child).sibling;
    if !(SEE!(v, '|' as c_int) || SEE!(v, stopper) || SEE!(v, EOS)) {
        /* parse all the rest of the branch, and insert in t->child->sibling */
        (*(*t).child).sibling = parsebranch(v, stopper, r#type, s2, rp, 1);
        NOERRN!(v);
        Assert!(SEE!(v, '|' as c_int) || SEE!(v, stopper) || SEE!(v, EOS));

        /* here's the promised update of the flags */
        (*t).flags |= COMBINE((*t).flags as c_int, (*(*(*t).child).sibling).flags as c_int)
            as c_char;
        (*top).flags |= COMBINE((*top).flags as c_int, (*t).flags as c_int) as c_char;

        /* neither t nor top could be directly marked for capture as yet */
        Assert!((*t).capno == 0);
        Assert!((*top).capno == 0);

        /*
         * At this point both top and t are concatenation (op == '.') subres,
         * and we have top->child = prefix of branch, top->child->sibling = t,
         * t->child = messy atom (with quantification superstructure if
         * needed), t->child->sibling = rest of branch.
         *
         * If the messy atom was the first thing in the branch, then
         * top->child is vacuous and we can get rid of one level of
         * concatenation.
         */
        Assert!((*(*top).child).op == '=' as c_char);
        if (*(*top).child).begin == (*(*top).child).end {
            Assert!(MESSY((*(*top).child).flags as c_int) == 0);
            freesubre(v, (*top).child);
            (*top).child = (*t).child;
            freesrnode(v, t);
        }
        /*
         * Otherwise, it's possible that t->child is not messy in itself, but
         * we considered it messy because its greediness conflicts with what
         * preceded it.  Then it could be that the combination of t->child and
         * the rest of the branch is also not messy, in which case we can get
         * rid of the child concatenation by merging t->child and the rest of
         * the branch into one plain DFA node.
         */
        else if (*(*t).child).op == '=' as c_char
            && (*(*(*t).child).sibling).op == '=' as c_char
            && MESSY(UP(
                (*(*t).child).flags as c_int | (*(*(*t).child).sibling).flags as c_int,
            )) == 0
        {
            (*t).op = '=' as c_char;
            (*t).flags = COMBINE(
                (*(*t).child).flags as c_int,
                (*(*(*t).child).sibling).flags as c_int,
            ) as c_char;
            freesubreandsiblings(v, (*t).child);
            (*t).child = std::ptr::null_mut();
        }
    } else {
        /*
         * There's nothing left in the branch, so we don't need the second
         * concatenation node 't'.  Just link s2 straight to rp.
         */
        EMPTYARC!(v, s2, rp);
        (*(*top).child).sibling = (*t).child;
        (*top).flags |=
            COMBINE((*top).flags as c_int, (*(*(*top).child).sibling).flags as c_int) as c_char;
        freesrnode(v, t);

        /*
         * Again, it could be that top->child is vacuous (if the messy atom
         * was in fact the only thing in the branch).  In that case we need no
         * concatenation at all; just replace top with top->child->sibling.
         */
        Assert!((*(*top).child).op == '=' as c_char);
        if (*(*top).child).begin == (*(*top).child).end {
            Assert!(MESSY((*(*top).child).flags as c_int) == 0);
            t = (*(*top).child).sibling;
            (*(*top).child).sibling = std::ptr::null_mut();
            freesubre(v, top);
            top = t;
        }
    }

    top
}

/*
 * nonword - generate arcs for non-word-character ahead or behind
 */
unsafe fn nonword(v: *mut vars, dir: c_int, lp: *mut state, rp: *mut state) {
    let anchor: c_int = if dir == AHEAD { '$' as c_int } else { '^' as c_int };

    Assert!(dir == AHEAD || dir == BEHIND);
    newarc((*v).nfa, anchor, 1, lp, rp);
    newarc((*v).nfa, anchor, 0, lp, rp);
    colorcomplement((*v).nfa, (*v).cm, dir, (*v).wordchrs, lp, rp);
    /* (no need for special attention to \n) */
}

/*
 * word - generate arcs for word character ahead or behind
 */
unsafe fn word(v: *mut vars, dir: c_int, lp: *mut state, rp: *mut state) {
    Assert!(dir == AHEAD || dir == BEHIND);
    cloneouts((*v).nfa, (*v).wordchrs, lp, rp, dir);
    /* (no need for special attention to \n) */
}

/*
 * charclass - generate arcs for a character class
 *
 * This is used for both atoms (\w and sibling escapes) and for elements
 * of bracket expressions.  The caller is responsible for calling okcolors()
 * at the end of processing the atom or bracket.
 */
unsafe fn charclass(v: *mut vars, cls: char_classes, lp: *mut state, rp: *mut state) {
    let cv: *mut cvec;

    /* obtain possibly-cached cvec for char class */
    NOTE!(v, REG_ULOCALE);
    cv = cclasscvec(v, cls, (*v).cflags & REG_ICASE);
    NOERR!(v);

    /* build the arcs; this may cause color splitting */
    subcolorcvec(v, cv, lp, rp);
}

/*
 * charclasscomplement - generate arcs for a complemented character class
 *
 * This is used for both atoms (\W and sibling escapes) and for elements
 * of bracket expressions.  In bracket expressions, it is the caller's
 * responsibility that there not be any open subcolors when this is called.
 */
unsafe fn charclasscomplement(
    v: *mut vars,
    cls: char_classes,
    lp: *mut state,
    rp: *mut state,
) {
    let cstate: *mut state;
    let cv: *mut cvec;

    /* make dummy state to hang temporary arcs on */
    cstate = newstate((*v).nfa);
    NOERR!(v);

    /* obtain possibly-cached cvec for char class */
    NOTE!(v, REG_ULOCALE);
    cv = cclasscvec(v, cls, (*v).cflags & REG_ICASE);
    NOERR!(v);

    /* build arcs for char class; this may cause color splitting */
    subcolorcvec(v, cv, cstate, cstate);
    NOERR!(v);

    /* clean up any subcolors in the arc set */
    okcolors((*v).nfa, (*v).cm);
    NOERR!(v);

    /* now build output arcs for the complement of the char class */
    colorcomplement((*v).nfa, (*v).cm, PLAIN, cstate, lp, rp);
    NOERR!(v);

    /* clean up dummy state */
    dropstate((*v).nfa, cstate);
}

/*
 * scannum - scan a number
 */
unsafe fn scannum(v: *mut vars) -> c_int {
    let mut n: c_int = 0;

    while SEE!(v, DIGIT) && n < DUPMAX {
        n = n * 10 + (*v).nextvalue as c_int;
        NEXT!(v);
    }
    if SEE!(v, DIGIT) || n > DUPMAX {
        ERR!(v, REG_BADBR);
        return 0;
    }
    n
}

/*
 * repeat - replicate subNFA for quantifiers
 *
 * The sub-NFA strung from lp to rp is modified to represent m to n
 * repetitions of its initial contents.
 *
 * The duplication sequences used here are chosen carefully so that any
 * pointers starting out pointing into the subexpression end up pointing into
 * the last occurrence.  (Note that it may not be strung between the same
 * left and right end states, however!)  This used to be important for the
 * subRE tree, although the important bits are now handled by the in-line
 * code in parse(), and when this is called, it doesn't matter any more.
 */
unsafe fn repeat(v: *mut vars, lp: *mut state, rp: *mut state, m: c_int, n: c_int) {
    // #define SOME 2 ; #define INF 3
    const SOME: c_int = 2;
    const INF: c_int = 3;
    // #define PAIR(x, y) ((x)*4 + (y))
    #[inline]
    fn PAIR(x: c_int, y: c_int) -> c_int {
        x * 4 + y
    }
    // #define REDUCE(x) ( ((x) == DUPINF) ? INF : (((x) > 1) ? SOME : (x)) )
    #[inline]
    fn REDUCE(x: c_int) -> c_int {
        if x == DUPINF {
            INF
        } else if x > 1 {
            SOME
        } else {
            x
        }
    }
    let rm: c_int = REDUCE(m);
    let rn: c_int = REDUCE(n);
    let mut s: *mut state;
    let s2: *mut state;

    let pr = PAIR(rm, rn);
    if pr == PAIR(0, 0) {
        /* empty string */
        delsub((*v).nfa, lp, rp);
        EMPTYARC!(v, lp, rp);
    } else if pr == PAIR(0, 1) {
        /* do as x| */
        EMPTYARC!(v, lp, rp);
    } else if pr == PAIR(0, SOME) {
        /* do as x{1,n}| */
        repeat(v, lp, rp, 1, n);
        NOERR!(v);
        EMPTYARC!(v, lp, rp);
    } else if pr == PAIR(0, INF) {
        /* loop x around */
        s = newstate((*v).nfa);
        NOERR!(v);
        moveouts((*v).nfa, lp, s);
        moveins((*v).nfa, rp, s);
        EMPTYARC!(v, lp, s);
        EMPTYARC!(v, s, rp);
    } else if pr == PAIR(1, 1) {
        /* no action required */
    } else if pr == PAIR(1, SOME) {
        /* do as x{0,n-1}x = (x{1,n-1}|)x */
        s = newstate((*v).nfa);
        NOERR!(v);
        moveouts((*v).nfa, lp, s);
        dupnfa((*v).nfa, s, rp, lp, s);
        NOERR!(v);
        repeat(v, lp, s, 1, n - 1);
        NOERR!(v);
        EMPTYARC!(v, lp, s);
    } else if pr == PAIR(1, INF) {
        /* add loopback arc */
        s = newstate((*v).nfa);
        s2 = newstate((*v).nfa);
        NOERR!(v);
        moveouts((*v).nfa, lp, s);
        moveins((*v).nfa, rp, s2);
        EMPTYARC!(v, lp, s);
        EMPTYARC!(v, s2, rp);
        EMPTYARC!(v, s2, s);
    } else if pr == PAIR(SOME, SOME) {
        /* do as x{m-1,n-1}x */
        s = newstate((*v).nfa);
        NOERR!(v);
        moveouts((*v).nfa, lp, s);
        dupnfa((*v).nfa, s, rp, lp, s);
        NOERR!(v);
        repeat(v, lp, s, m - 1, n - 1);
    } else if pr == PAIR(SOME, INF) {
        /* do as x{m-1,}x */
        s = newstate((*v).nfa);
        NOERR!(v);
        moveouts((*v).nfa, lp, s);
        dupnfa((*v).nfa, s, rp, lp, s);
        NOERR!(v);
        repeat(v, lp, s, m - 1, n);
    } else {
        ERR!(v, REG_ASSERT);
    }
}

/*
 * bracket - handle non-complemented bracket expression
 *
 * Also called from cbracket for complemented bracket expressions.
 */
unsafe fn bracket(v: *mut vars, lp: *mut state, rp: *mut state) {
    /*
     * We can't process complemented char classes (e.g. \W) immediately while
     * scanning the bracket expression, else color bookkeeping gets confused.
     * Instead, remember whether we saw any in have_cclassc[], and process
     * them at the end.
     */
    let mut have_cclassc: [bool; NUM_CCLASSES] = [false; NUM_CCLASSES];
    let mut any_cclassc: bool;
    let mut i: c_int;

    Assert!(SEE!(v, '[' as c_int));
    NEXT!(v);
    while !SEE!(v, ']' as c_int) && !SEE!(v, EOS) {
        brackpart(v, lp, rp, have_cclassc.as_mut_ptr());
    }
    Assert!(SEE!(v, ']' as c_int) || ISERR!(v));

    /* close up open subcolors from the positive bracket elements */
    okcolors((*v).nfa, (*v).cm);
    NOERR!(v);

    /* now handle any complemented elements */
    any_cclassc = false;
    i = 0;
    while (i as usize) < NUM_CCLASSES {
        if have_cclassc[i as usize] {
            charclasscomplement(v, i as char_classes, lp, rp);
            NOERR!(v);
            any_cclassc = true;
        }
        i += 1;
    }

    /*
     * If we had any complemented elements, see if we can optimize the bracket
     * into a rainbow.  Since a complemented element is the only way a WHITE
     * arc could get into the result, there's no point in checking otherwise.
     */
    if any_cclassc {
        optimizebracket(v, lp, rp);
    }
}

/*
 * cbracket - handle complemented bracket expression
 *
 * We do it by calling bracket() with dummy endpoints, and then complementing
 * the result.  The alternative would be to invoke rainbow(), and then delete
 * arcs as the b.e. is seen... but that gets messy, and is really quite
 * infeasible now that rainbow() just puts out one RAINBOW arc.
 */
unsafe fn cbracket(v: *mut vars, lp: *mut state, rp: *mut state) {
    let left: *mut state = newstate((*v).nfa);
    let right: *mut state = newstate((*v).nfa);

    NOERR!(v);
    bracket(v, left, right);

    /* in NLSTOP mode, ensure newline is not part of the result set */
    if (*v).cflags & REG_NLSTOP != 0 {
        newarc((*v).nfa, PLAIN, (*v).nlcolor, left, right);
    }
    NOERR!(v);

    Assert!((*lp).nouts == 0); /* all outarcs will be ours */

    /*
     * Easy part of complementing, and all there is to do since the MCCE code
     * was removed.  Note that the result of colorcomplement() cannot be a
     * rainbow, since we don't allow empty brackets; so there's no point in
     * calling optimizebracket() again.
     */
    colorcomplement((*v).nfa, (*v).cm, PLAIN, left, lp, rp);
    NOERR!(v);
    dropstate((*v).nfa, left);
    Assert!((*right).nins == 0);
    freestate((*v).nfa, right);
}

/*
 * brackpart - handle one item (or range) within a bracket expression
 */
unsafe fn brackpart(
    v: *mut vars,
    lp: *mut state,
    rp: *mut state,
    have_cclassc: *mut bool,
) {
    let mut startc: chr;
    let endc: chr;
    let cv: *mut cvec;
    let cls: char_classes;
    let mut startp: *const chr;
    let mut endp: *const chr;

    /* parse something, get rid of special cases, take shortcuts */
    match (*v).nexttype {
        x if x == RANGE => {
            /* a-b-c or other botch */
            ERR!(v, REG_ERANGE);
            return;
        }
        x if x == PLAIN => {
            startc = (*v).nextvalue;
            NEXT!(v);
            /* shortcut for ordinary chr (not range) */
            if !SEE!(v, RANGE) {
                onechr(v, startc, lp, rp);
                return;
            }
            NOERR!(v);
        }
        x if x == COLLEL => {
            startp = (*v).now;
            endp = scanplain(v);
            INSIST!(v, startp < endp, REG_ECOLLATE);
            NOERR!(v);
            startc = element(v, startp, endp);
            NOERR!(v);
        }
        x if x == ECLASS => {
            startp = (*v).now;
            endp = scanplain(v);
            INSIST!(v, startp < endp, REG_ECOLLATE);
            NOERR!(v);
            startc = element(v, startp, endp);
            NOERR!(v);
            cv = eclass(v, startc, (*v).cflags & REG_ICASE);
            NOERR!(v);
            subcolorcvec(v, cv, lp, rp);
            return;
        }
        x if x == CCLASS => {
            startp = (*v).now;
            endp = scanplain(v);
            INSIST!(v, startp < endp, REG_ECTYPE);
            NOERR!(v);
            cls = lookupcclass(v, startp, endp);
            NOERR!(v);
            charclass(v, cls, lp, rp);
            return;
        }
        x if x == CCLASSS => {
            charclass(v, (*v).nextvalue as char_classes, lp, rp);
            NEXT!(v);
            return;
        }
        x if x == CCLASSC => {
            /* we cannot call charclasscomplement() immediately */
            *have_cclassc.add((*v).nextvalue as usize) = true;
            NEXT!(v);
            return;
        }
        _ => {
            ERR!(v, REG_ASSERT);
            return;
        }
    }

    if SEE!(v, RANGE) {
        NEXT!(v);
        match (*v).nexttype {
            x if x == PLAIN || x == RANGE => {
                endc = (*v).nextvalue;
                NEXT!(v);
                NOERR!(v);
            }
            x if x == COLLEL => {
                startp = (*v).now;
                endp = scanplain(v);
                INSIST!(v, startp < endp, REG_ECOLLATE);
                NOERR!(v);
                endc = element(v, startp, endp);
                NOERR!(v);
            }
            _ => {
                ERR!(v, REG_ERANGE);
                return;
            }
        }
    } else {
        endc = startc;
    }

    /*
     * Ranges are unportable.  Actually, standard C does guarantee that digits
     * are contiguous, but making that an exception is just too complicated.
     */
    if startc != endc {
        NOTE!(v, REG_UUNPORT);
    }
    cv = range(v, startc, endc, (*v).cflags & REG_ICASE);
    NOERR!(v);
    subcolorcvec(v, cv, lp, rp);
}

/*
 * scanplain - scan PLAIN contents of [. etc.
 *
 * Certain bits of trickery in regc_lex.c know that this code does not try
 * to look past the final bracket of the [. etc.
 */
unsafe fn scanplain(v: *mut vars) -> *const chr {
    let mut endp: *const chr;

    Assert!(SEE!(v, COLLEL) || SEE!(v, ECLASS) || SEE!(v, CCLASS));
    NEXT!(v);

    endp = (*v).now;
    while SEE!(v, PLAIN) {
        endp = (*v).now;
        NEXT!(v);
    }

    Assert!(SEE!(v, END) || ISERR!(v));
    NEXT!(v);

    endp
}

/*
 * onechr - fill in arcs for a plain character, and possible case complements
 * This is mostly a shortcut for efficient handling of the common case.
 */
unsafe fn onechr(v: *mut vars, c: chr, lp: *mut state, rp: *mut state) {
    if (*v).cflags & REG_ICASE == 0 {
        let mut lastsubcolor: color = COLORLESS;

        subcoloronechr(v, c, lp, rp, &mut lastsubcolor);
        return;
    }

    /* rats, need general case anyway... */
    subcolorcvec(v, allcases(v, c), lp, rp);
}

/*
 * optimizebracket - see if bracket expression can be converted to RAINBOW
 *
 * Cases such as "[\s\S]" can produce a set of arcs of all colors, which we
 * can replace by a single RAINBOW arc for efficiency.  (This might seem
 * like a silly way to write ".", but it's seemingly a common locution in
 * some other flavors of regex, so take the trouble to support it well.)
 */
unsafe fn optimizebracket(v: *mut vars, lp: *mut state, rp: *mut state) {
    let mut cd: *mut colordesc;
    let end: *mut colordesc = CDEND((*v).cm);
    let mut a: *mut arc;
    let mut israinbow: bool;

    /*
     * Scan lp's out-arcs and transiently mark the mentioned colors.  We
     * expect that all of lp's out-arcs are plain, non-RAINBOW arcs to rp.
     * (Note: there shouldn't be any pseudocolors yet, but check anyway.)
     */
    a = (*lp).outs;
    while !a.is_null() {
        Assert!((*a).r#type == PLAIN);
        Assert!((*a).co >= 0); /* i.e. not RAINBOW */
        Assert!((*a).to == rp);
        cd = (*(*v).cm).cd.add((*a).co as usize);
        Assert!(UNUSEDCOLOR(cd) == 0 && ((*cd).flags & PSEUDO) == 0);
        (*cd).flags |= COLMARK;
        a = (*a).outchain;
    }

    /* Scan colors, clear transient marks, check for unmarked live colors */
    israinbow = true;
    cd = (*(*v).cm).cd;
    while cd < end {
        if (*cd).flags & COLMARK != 0 {
            (*cd).flags &= !COLMARK;
        } else if UNUSEDCOLOR(cd) == 0 && ((*cd).flags & PSEUDO) == 0 {
            israinbow = false;
        }
        cd = cd.add(1);
    }

    /* Can't do anything if not all colors have arcs */
    if !israinbow {
        return;
    }

    /* OK, drop existing arcs and replace with a rainbow */
    loop {
        a = (*lp).outs;
        if a.is_null() {
            break;
        }
        freearc((*v).nfa, a);
    }
    newarc((*v).nfa, PLAIN, RAINBOW, lp, rp);
}

/*
 * wordchrs - set up word-chr list for word-boundary stuff, if needed
 *
 * The list is kept as a bunch of circular arcs on an otherwise-unused state.
 *
 * Note that this must not be called while we have any open subcolors,
 * else construction of the list would confuse color bookkeeping.
 * Hence, we can't currently apply a similar optimization in
 * charclass[complement](), as those need to be usable within bracket
 * expressions.
 */
unsafe fn wordchrs(v: *mut vars) {
    let cstate: *mut state;
    let cv: *mut cvec;

    if !(*v).wordchrs.is_null() {
        return; /* done already */
    }

    /* make dummy state to hang the cache arcs on */
    cstate = newstate((*v).nfa);
    NOERR!(v);

    /* obtain possibly-cached cvec for \w characters */
    NOTE!(v, REG_ULOCALE);
    cv = cclasscvec(v, CC_WORD, (*v).cflags & REG_ICASE);
    NOERR!(v);

    /* build the arcs; this may cause color splitting */
    subcolorcvec(v, cv, cstate, cstate);
    NOERR!(v);

    /* close new open subcolors to ensure the cache entry is self-contained */
    okcolors((*v).nfa, (*v).cm);
    NOERR!(v);

    /* success! save the cache pointer */
    (*v).wordchrs = cstate;
}

/*
 * processlacon - generate the NFA representation of a LACON
 *
 * In the general case this is just newlacon() + newarc(), but some cases
 * can be optimized.
 */
unsafe fn processlacon(
    v: *mut vars,
    begin: *mut state, /* start of parsed LACON sub-re */
    end: *mut state,   /* end of parsed LACON sub-re */
    latype: c_int,
    lp: *mut state, /* left state to hang it on */
    rp: *mut state, /* right state to hang it on */
) {
    let s1: *mut state;
    let n: c_int;

    /*
     * Check for lookaround RE consisting of a single plain color arc (or set
     * of arcs); this would typically be a simple chr or a bracket expression.
     */
    s1 = single_color_transition(begin, end);
    match latype {
        x if x == LATYPE_AHEAD_POS => {
            /* If lookahead RE is just colorset C, convert to AHEAD(C) */
            if !s1.is_null() {
                cloneouts((*v).nfa, s1, lp, rp, AHEAD);
                return;
            }
        }
        x if x == LATYPE_AHEAD_NEG => {
            /* If lookahead RE is just colorset C, convert to AHEAD(^C)|$ */
            if !s1.is_null() {
                colorcomplement((*v).nfa, (*v).cm, AHEAD, s1, lp, rp);
                newarc((*v).nfa, '$' as c_int, 1, lp, rp);
                newarc((*v).nfa, '$' as c_int, 0, lp, rp);
                return;
            }
        }
        x if x == LATYPE_BEHIND_POS => {
            /* If lookbehind RE is just colorset C, convert to BEHIND(C) */
            if !s1.is_null() {
                cloneouts((*v).nfa, s1, lp, rp, BEHIND);
                return;
            }
        }
        x if x == LATYPE_BEHIND_NEG => {
            /* If lookbehind RE is just colorset C, convert to BEHIND(^C)|^ */
            if !s1.is_null() {
                colorcomplement((*v).nfa, (*v).cm, BEHIND, s1, lp, rp);
                newarc((*v).nfa, '^' as c_int, 1, lp, rp);
                newarc((*v).nfa, '^' as c_int, 0, lp, rp);
                return;
            }
        }
        _ => {
            Assert!(NOTREACHED != 0);
        }
    }

    /* General case: we need a LACON subre and arc */
    n = newlacon(v, begin, end, latype);
    newarc((*v).nfa, LACON, n as color, lp, rp);
}

/*
 * subre - allocate a subre
 */
unsafe fn subre(
    v: *mut vars,
    op: c_int,
    flags: c_int,
    begin: *mut state,
    end: *mut state,
) -> *mut subre {
    let ret: *mut subre = (*v).treefree;

    /*
     * Checking for stack overflow here is sufficient to protect parse() and
     * its recursive subroutines.
     */
    if STACK_TOO_DEEP((*(*v).re).re_fns as *mut fns) != 0 {
        ERR!(v, REG_ETOOBIG);
        return std::ptr::null_mut();
    }

    let ret: *mut subre = if !ret.is_null() {
        (*v).treefree = (*ret).child;
        ret
    } else {
        let r = MALLOC(std::mem::size_of::<subre>()) as *mut subre;
        if r.is_null() {
            ERR!(v, REG_ESPACE);
            return std::ptr::null_mut();
        }
        (*r).chain = (*v).treechain;
        (*v).treechain = r;
        r
    };

    Assert!(!strchr_eq(b"=b|.*(", op));

    (*ret).op = op as c_char;
    (*ret).flags = flags as c_char;
    (*ret).latype = -1i8 as c_char;
    (*ret).id = 0; /* will be assigned later */
    (*ret).capno = 0;
    (*ret).backno = 0;
    (*ret).min = 1;
    (*ret).max = 1;
    (*ret).child = std::ptr::null_mut();
    (*ret).sibling = std::ptr::null_mut();
    (*ret).begin = begin;
    (*ret).end = end;
    ZAPCNFA(&mut (*ret).cnfa);

    ret
}

// helper for the assert strchr("=b|.*(", op) != NULL: returns true if op is in
// the set (i.e. strchr would be NULL when not found, so the Assert checks the
// negation of "not found").
#[inline]
fn strchr_eq(set: &[u8], op: c_int) -> bool {
    !set.iter().any(|&b| b as c_int == op)
}

/*
 * freesubre - free a subRE subtree
 *
 * This frees child node(s) of the given subRE too,
 * but not its siblings.
 */
unsafe fn freesubre(v: *mut vars /* might be NULL */, sr: *mut subre) {
    if sr.is_null() {
        return;
    }

    if !(*sr).child.is_null() {
        freesubreandsiblings(v, (*sr).child);
    }

    freesrnode(v, sr);
}

/*
 * freesubreandsiblings - free a subRE subtree
 *
 * This frees child node(s) of the given subRE too,
 * as well as any following siblings.
 */
unsafe fn freesubreandsiblings(v: *mut vars /* might be NULL */, mut sr: *mut subre) {
    while !sr.is_null() {
        let next: *mut subre = (*sr).sibling;

        freesubre(v, sr);
        sr = next;
    }
}

/*
 * freesrnode - free one node in a subRE subtree
 */
unsafe fn freesrnode(v: *mut vars /* might be NULL */, sr: *mut subre) {
    if sr.is_null() {
        return;
    }

    if !NULLCNFA(&(*sr).cnfa) {
        freecnfa(&mut (*sr).cnfa);
    }
    (*sr).flags = 0; /* in particular, not INUSE */
    (*sr).child = std::ptr::null_mut();
    (*sr).sibling = std::ptr::null_mut();
    (*sr).begin = std::ptr::null_mut();
    (*sr).end = std::ptr::null_mut();

    if !v.is_null() && !(*v).treechain.is_null() {
        /* we're still parsing, maybe we can reuse the subre */
        (*sr).child = (*v).treefree;
        (*v).treefree = sr;
    } else {
        FREE(sr as *mut c_void);
    }
}

/*
 * removecaptures - remove unnecessary capture subREs
 *
 * If the caller said that it doesn't care about subexpression match data,
 * we may delete the "capture" markers on subREs that are not referenced
 * by any backrefs, and then simplify anything that's become non-messy.
 * Call this only if REG_NOSUB flag is set.
 */
unsafe fn removecaptures(v: *mut vars, t: *mut subre) {
    let mut t2: *mut subre;

    Assert!(!t.is_null());

    /*
     * If this isn't itself a backref target, clear capno and tentatively
     * clear CAP flag.
     */
    if (*t).flags as c_int & BRUSE == 0 {
        (*t).capno = 0;
        (*t).flags &= !CAP as c_char;
    }

    /* Now recurse to children */
    t2 = (*t).child;
    while !t2.is_null() {
        removecaptures(v, t2);
        /* Propagate child CAP flag back up, if it's still set */
        if (*t2).flags as c_int & CAP != 0 {
            (*t).flags |= CAP as c_char;
        }
        t2 = (*t2).sibling;
    }

    /*
     * If t now contains neither captures nor backrefs, there's no longer any
     * need to care where its sub-match boundaries are, so we can reduce it to
     * a simple DFA node.  (Note in particular that MIXED child greediness is
     * not a hindrance here, so we don't use the MESSY() macro.)
     */
    if (*t).flags as c_int & (CAP | BACKR) == 0 {
        if !(*t).child.is_null() {
            freesubreandsiblings(v, (*t).child);
        }
        (*t).child = std::ptr::null_mut();
        (*t).op = '=' as c_char;
        (*t).flags &= !MIXED as c_char;
    }
}

/*
 * numst - number tree nodes (assigning "id" indexes)
 */
unsafe fn numst(t: *mut subre, start: c_int) -> c_int {
    let mut i: c_int;
    let mut t2: *mut subre;

    Assert!(!t.is_null());

    i = start;
    (*t).id = i;
    i += 1;
    t2 = (*t).child;
    while !t2.is_null() {
        i = numst(t2, i);
        t2 = (*t2).sibling;
    }
    i
}

/*
 * markst - mark tree nodes as INUSE
 *
 * Note: this is a great deal more subtle than it looks.  During initial
 * parsing of a regex, all subres are linked into the treechain list;
 * discarded ones are also linked into the treefree list for possible reuse.
 * After we are done creating all subres required for a regex, we run markst()
 * then cleanst(), which results in discarding all subres not reachable from
 * v->tree.  We then clear v->treechain, indicating that subres must be found
 * by descending from v->tree.  This changes the behavior of freesubre(): it
 * will henceforth FREE() unwanted subres rather than sticking them into the
 * treefree list.  (Doing that any earlier would result in dangling links in
 * the treechain list.)  This all means that freev() will clean up correctly
 * if invoked before or after markst()+cleanst(); but it would not work if
 * called partway through this state conversion, so we mustn't error out
 * in or between these two functions.
 */
unsafe fn markst(t: *mut subre) {
    let mut t2: *mut subre;

    Assert!(!t.is_null());

    (*t).flags |= INUSE as c_char;
    t2 = (*t).child;
    while !t2.is_null() {
        markst(t2);
        t2 = (*t2).sibling;
    }
}

/*
 * cleanst - free any tree nodes not marked INUSE
 */
unsafe fn cleanst(v: *mut vars) {
    let mut t: *mut subre;
    let mut next: *mut subre;

    t = (*v).treechain;
    while !t.is_null() {
        next = (*t).chain;
        if (*t).flags as c_int & INUSE == 0 {
            FREE(t as *mut c_void);
        }
        t = next;
    }
    (*v).treechain = std::ptr::null_mut();
    (*v).treefree = std::ptr::null_mut(); /* just on general principles */
}

/*
 * nfatree - turn a subRE subtree into a tree of compacted NFAs
 */
unsafe fn nfatree(v: *mut vars, t: *mut subre, f: *mut c_void /* FILE * */) -> c_long {
    let mut t2: *mut subre;

    Assert!(!t.is_null() && !(*t).begin.is_null());

    t2 = (*t).child;
    while !t2.is_null() {
        let _: c_long = nfatree(v, t2, f); /* (DISCARD) */
        t2 = (*t2).sibling;
    }

    nfanode(v, t, 0, f)
}

/*
 * nfanode - do one NFA for nfatree or lacons
 *
 * If converttosearch is true, apply makesearch() to the NFA.
 */
unsafe fn nfanode(
    v: *mut vars,
    t: *mut subre,
    converttosearch: c_int,
    f: *mut c_void, /* FILE * for debug output */
) -> c_long {
    let nfa: *mut nfa;
    let mut ret: c_long = 0;
    let _ = f;

    Assert!(!(*t).begin.is_null());

    nfa = newnfa(v, (*v).cm, (*v).nfa);
    NOERRZ!(v);
    dupnfa(nfa, (*t).begin, (*t).end, (*nfa).init, (*nfa).r#final);
    (*nfa).flags = (*(*v).nfa).flags;
    if !ISERR!(v) {
        specialcolors(nfa);
    }
    if !ISERR!(v) {
        ret = optimize(nfa, f);
    }
    if converttosearch != 0 && !ISERR!(v) {
        makesearch(v, nfa);
    }
    if !ISERR!(v) {
        compact(nfa, &mut (*t).cnfa);
    }

    freenfa(nfa);
    ret
}

/*
 * newlacon - allocate a lookaround-constraint subRE
 */
unsafe fn newlacon(
    v: *mut vars,
    begin: *mut state,
    end: *mut state,
    latype: c_int,
) -> c_int {
    let n: c_int;
    let newlacons: *mut subre;
    let sub: *mut subre;

    if (*v).nlacons == 0 {
        n = 1; /* skip 0th */
        newlacons = MALLOC(2 * std::mem::size_of::<subre>()) as *mut subre;
    } else {
        n = (*v).nlacons;
        newlacons = REALLOC(
            (*v).lacons as *mut c_void,
            (n as usize + 1) * std::mem::size_of::<subre>(),
        ) as *mut subre;
    }
    if newlacons.is_null() {
        ERR!(v, REG_ESPACE);
        return 0;
    }
    (*v).lacons = newlacons;
    (*v).nlacons = n + 1;
    sub = (*v).lacons.add(n as usize);
    (*sub).begin = begin;
    (*sub).end = end;
    (*sub).latype = latype as c_char;
    ZAPCNFA(&mut (*sub).cnfa);
    n
}

/*
 * freelacons - free lookaround-constraint subRE vector
 */
unsafe fn freelacons(subs: *mut subre, n: c_int) {
    let mut sub: *mut subre;
    let mut i: c_int;

    Assert!(n > 0);
    sub = subs.add(1);
    i = n - 1;
    while i > 0 {
        /* no 0th */
        if !NULLCNFA(&(*sub).cnfa) {
            freecnfa(&mut (*sub).cnfa);
        }
        sub = sub.add(1);
        i -= 1;
    }
    FREE(subs as *mut c_void);
}

/*
 * rfree - free a whole RE (insides of regfree)
 */
unsafe fn rfree(re: *mut regex_t) {
    let g: *mut guts;

    if re.is_null() || (*re).re_magic != REMAGIC {
        return;
    }

    (*re).re_magic = 0; /* invalidate RE */
    g = (*re).re_guts as *mut guts;
    (*re).re_guts = std::ptr::null_mut();
    (*re).re_fns = std::ptr::null_mut();
    if !g.is_null() {
        (*g).magic = 0;
        freecm(&mut (*g).cmap);
        if !(*g).tree.is_null() {
            freesubre(std::ptr::null_mut(), (*g).tree);
        }
        if !(*g).lacons.is_null() {
            freelacons((*g).lacons, (*g).nlacons);
        }
        if !NULLCNFA(&(*g).search) {
            freecnfa(&mut (*g).search);
        }
        FREE(g as *mut c_void);
    }
}

/*
 * rstacktoodeep - check for stack getting dangerously deep
 *
 * Return nonzero to fail the operation with error code REG_ETOOBIG,
 * zero to keep going
 *
 * The current implementation is Postgres-specific.  If we ever get around
 * to splitting the regex code out as a standalone library, there will need
 * to be some API to let applications define a callback function for this.
 */
unsafe extern "C" fn rstacktoodeep() -> c_int {
    stack_is_too_deep() as c_int
}

// ---------------------------------------------------------------------------
// Cross-module dependencies.
//
// These functions live in the other regc_*.c files that regcomp.c #includes
// (regc_lex.c, regc_color.c, regc_nfa.c, regc_cvec.c, regc_pg_locale.c,
// regc_locale.c).  They are not yet translated; provide TODO(pg-port) stub
// bodies so this module type-checks.  Replace with `use` imports once the
// sibling modules exist.
// ---------------------------------------------------------------------------

// === regc_lex.c ===
unsafe fn lexstart(_v: *mut vars) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn next(_v: *mut vars) -> c_int {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn newline() -> chr {
    /* TODO(pg-port) */
    unimplemented!()
}

// === regc_color.c ===
unsafe fn initcm(_v: *mut vars, _cm: *mut colormap) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn freecm(_cm: *mut colormap) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn subcolor(_cm: *mut colormap, _c: chr) -> color {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn subcolorcvec(_v: *mut vars, _cv: *mut cvec, _lp: *mut state, _rp: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn subcoloronechr(
    _v: *mut vars,
    _ch: chr,
    _lp: *mut state,
    _rp: *mut state,
    _lastsubcolor: *mut color,
) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn okcolors(_nfa: *mut nfa, _cm: *mut colormap) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn rainbow(
    _nfa: *mut nfa,
    _cm: *mut colormap,
    _type: c_int,
    _but: color,
    _from: *mut state,
    _to: *mut state,
) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn colorcomplement(
    _nfa: *mut nfa,
    _cm: *mut colormap,
    _type: c_int,
    _of: *mut state,
    _from: *mut state,
    _to: *mut state,
) {
    /* TODO(pg-port) */
    unimplemented!()
}

// === regc_nfa.c ===
unsafe fn newnfa(_v: *mut vars, _cm: *mut colormap, _parent: *mut nfa) -> *mut nfa {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn freenfa(_nfa: *mut nfa) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn newstate(_nfa: *mut nfa) -> *mut state {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn dropstate(_nfa: *mut nfa, _s: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn freestate(_nfa: *mut nfa, _s: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn newarc(_nfa: *mut nfa, _t: c_int, _co: color, _from: *mut state, _to: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn freearc(_nfa: *mut nfa, _victim: *mut arc) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn cparc(_nfa: *mut nfa, _oa: *mut arc, _from: *mut state, _to: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn moveins(_nfa: *mut nfa, _old_state: *mut state, _new_state: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn copyouts(_nfa: *mut nfa, _old_state: *mut state, _new_state: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn moveouts(_nfa: *mut nfa, _old_state: *mut state, _new_state: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn cloneouts(
    _nfa: *mut nfa,
    _old: *mut state,
    _from: *mut state,
    _to: *mut state,
    _type: c_int,
) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn delsub(_nfa: *mut nfa, _lp: *mut state, _rp: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn dupnfa(
    _nfa: *mut nfa,
    _start: *mut state,
    _stop: *mut state,
    _from: *mut state,
    _to: *mut state,
) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn removeconstraints(_nfa: *mut nfa, _start: *mut state, _stop: *mut state) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn single_color_transition(_s1: *mut state, _s2: *mut state) -> *mut state {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn specialcolors(_nfa: *mut nfa) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn optimize(_nfa: *mut nfa, _f: *mut c_void) -> c_long {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn compact(_nfa: *mut nfa, _cnfa: *mut cnfa) {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn freecnfa(_cnfa: *mut cnfa) {
    /* TODO(pg-port) */
    unimplemented!()
}

// === regc_cvec.c ===
unsafe fn newcvec(_nchrs: c_int, _nranges: c_int) -> *mut cvec {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn freecvec(_cv: *mut cvec) {
    /* TODO(pg-port) */
    unimplemented!()
}

// === regc_locale.c ===
unsafe fn element(_v: *mut vars, _startp: *const chr, _endp: *const chr) -> chr {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn range(_v: *mut vars, _a: chr, _b: chr, _cases: c_int) -> *mut cvec {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn eclass(_v: *mut vars, _c: chr, _cases: c_int) -> *mut cvec {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn lookupcclass(_v: *mut vars, _startp: *const chr, _endp: *const chr) -> char_classes {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn cclasscvec(_v: *mut vars, _cclasscode: char_classes, _cases: c_int) -> *mut cvec {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe fn allcases(_v: *mut vars, _c: chr) -> *mut cvec {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe extern "C" fn cmp(_x: *const chr, _y: *const chr, _len: usize) -> c_int {
    /* TODO(pg-port) */
    unimplemented!()
}
unsafe extern "C" fn casecmp(_x: *const chr, _y: *const chr, _len: usize) -> c_int {
    /* TODO(pg-port) */
    unimplemented!()
}

// === regguts.h exported prototypes (regc_pg_locale.c) ===
unsafe fn pg_set_regex_collation(_collation: Oid) {
    /* TODO(pg-port) */
    unimplemented!()
}
