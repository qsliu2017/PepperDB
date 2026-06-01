//! re_*exec and friends - match REs
//!
//! Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
//!
//! Development of this software was funded, in part, by Cray Research Inc.,
//! UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
//! Corporation, none of whom are responsible for the results.  The author
//! thanks all of them.
//!
//! Translated 1:1 from postgres/src/backend/regex/regexec.c
//!
//! In the C build this file #includes rege_dfa.c at the end; in the Rust port
//! the DFA routines (longest, shortest, matchuntil, dfa_backref, lastcold,
//! newdfa, freedfa, hash, initialize, miss, lacon, getvacant, pickss) live in
//! this same module, mirroring the single translation unit.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(unused_assignments)]

use std::ffi::{c_char, c_int, c_uint, c_void};

use crate::c::Size;
use crate::regex::regcustom::chr;
use crate::regex::regex::{
    regex_t, regmatch_t, regoff_t, rm_detail_t, REG_EXPECT, REG_NOSUB, REG_NOTBOL, REG_NOTEOL,
    REG_SMALL,
};
use crate::regex::regerror::{
    REG_ASSERT, REG_ESPACE, REG_ETOOBIG, REG_INVARG, REG_MIXED, REG_NOMATCH, REG_OKAY,
};
use crate::regex::regex::{REG_UBACKREF, REG_UIMPOSSIBLE};
use crate::regex::regguts::{
    cnfa, color, colormap, fns, guts, subre, carc, colordesc, pg_set_regex_collation, GETCOLOR,
    BACKR, CNFA_NOPROGRESS, COLORLESS, DUPINF, HASLACONS, LATYPE_IS_AHEAD, LATYPE_IS_POS, MATCHALL,
    PSEUDO, RAINBOW, REMAGIC, SHORTER, STACK_TOO_DEEP, WHITE,
};
use crate::utils::palloc::{palloc_extended, pfree, MCXT_ALLOC_NO_OOM};

use crate::Assert;

// ---------------------------------------------------------------------------
// regcustom.h allocator / interrupt macros, expressed at the use sites in C.
// ---------------------------------------------------------------------------

/// C: #define MALLOC(n) palloc_extended((n), MCXT_ALLOC_NO_OOM)
unsafe fn MALLOC(n: usize) -> *mut c_void {
    palloc_extended(n, MCXT_ALLOC_NO_OOM)
}

/// C: #define FREE(p) pfree(VS(p))
unsafe fn FREE(p: *mut c_void) {
    pfree(p);
}

/// C: #define INTERRUPT(re) CHECK_FOR_INTERRUPTS()
unsafe fn INTERRUPT(_re: *mut regex_t) {
    crate::miscadmin::CHECK_FOR_INTERRUPTS();
}

// ---------------------------------------------------------------------------
// lazy-DFA representation
// ---------------------------------------------------------------------------

/// "pointer" to an outarc
#[repr(C)]
#[derive(Clone, Copy)]
struct arcp {
    ss: *mut sset,
    co: color,
}

/// state set
#[repr(C)]
struct sset {
    /// pointer to bitvector
    states: *mut c_uint,
    /// hash of bitvector
    hash: c_uint,
    flags: c_int,
    /// chain of inarcs pointing here
    ins: arcp,
    /// last entered on arrival here
    lastseen: *mut chr,
    /// outarc vector indexed by color
    outs: *mut *mut sset,
    /// chain-pointer vector for outarcs
    inchain: *mut arcp,
}

// #define  HASH(bv, nw)  (((nw) == 1) ? *(bv) : hash(bv, nw))
#[inline]
unsafe fn HASH(bv: *mut c_uint, nw: c_int) -> c_uint {
    if nw == 1 {
        *bv
    } else {
        hash(bv, nw)
    }
}

// #define  HIT(h,bv,ss,nw) ((ss)->hash == (h) && ((nw) == 1 ||
//     memcmp(VS(bv), VS((ss)->states), (nw)*sizeof(unsigned)) == 0))
#[inline]
unsafe fn HIT(h: c_uint, bv: *mut c_uint, ss: *mut sset, nw: c_int) -> bool {
    (*ss).hash == h
        && (nw == 1
            || {
                let mut eq = true;
                let mut i = 0;
                while i < nw as isize {
                    if *bv.offset(i) != *(*ss).states.offset(i) {
                        eq = false;
                        break;
                    }
                    i += 1;
                }
                eq
            })
}

/* sset.flags bits */
/// the initial state set
const STARTER: c_int = 0o1;
/// includes the goal state
const POSTSTATE: c_int = 0o2;
/// locked in cache
const LOCKED: c_int = 0o4;
/// zero-progress state set
const NOPROGRESS: c_int = 0o10;

#[repr(C)]
struct dfa {
    /// size of cache
    nssets: c_int,
    /// how many entries occupied yet
    nssused: c_int,
    /// number of states
    nstates: c_int,
    /// length of outarc and inchain vectors
    ncolors: c_int,
    /// length of state-set bitvectors
    wordsper: c_int,
    /// state-set cache
    ssets: *mut sset,
    /// bitvector storage
    statesarea: *mut c_uint,
    /// pointer to work area within statesarea
    work: *mut c_uint,
    /// outarc-vector storage
    outsarea: *mut *mut sset,
    /// inchain storage
    incarea: *mut arcp,
    cnfa: *mut cnfa,
    cm: *mut colormap,
    /// location of last cache-flushed success
    lastpost: *mut chr,
    /// location of last cache-flushed NOPROGRESS
    lastnopr: *mut chr,
    /// replacement-search-pointer memory
    search: *mut sset,
    /// if DFA for a backref, subno it refers to
    backno: c_int,
    /// min repetitions for backref
    backmin: i16,
    /// max repetitions for backref
    backmax: i16,
    /// should this struct dfa be freed?
    ismalloced: bool,
    /// should its subsidiary arrays be freed?
    arraysmalloced: bool,
}

/// number of work bitvectors needed
const WORK: c_int = 1;

/* setup for non-malloc allocation for small cases */
/// must be less than UBITS
const FEWSTATES: c_int = 20;
const FEWCOLORS: c_int = 15;

#[repr(C)]
struct smalldfa {
    /// must be first
    dfa: dfa,
    ssets: [sset; (FEWSTATES * 2) as usize],
    statesarea: [c_uint; (FEWSTATES * 2 + WORK) as usize],
    outsarea: [*mut sset; (FEWSTATES * 2 * FEWCOLORS) as usize],
    incarea: [arcp; (FEWSTATES * 2 * FEWCOLORS) as usize],
}

/// #define DOMALLOC ((struct smalldfa *)NULL) -- force malloc
const DOMALLOC: *mut smalldfa = std::ptr::null_mut();

/// #define UBITS (CHAR_BIT * sizeof(unsigned))
const UBITS: c_int = 8 * (core::mem::size_of::<c_uint>() as c_int);

// #define BSET(uv, sn) ((uv)[(sn)/UBITS] |= (unsigned)1 << ((sn)%UBITS))
#[inline]
unsafe fn BSET(uv: *mut c_uint, sn: c_int) {
    *uv.offset((sn / UBITS) as isize) |= 1u32 << (sn % UBITS);
}

// #define ISBSET(uv, sn) ((uv)[(sn)/UBITS] & ((unsigned)1 << ((sn)%UBITS)))
#[inline]
unsafe fn ISBSET(uv: *mut c_uint, sn: c_int) -> c_uint {
    *uv.offset((sn / UBITS) as isize) & (1u32 << (sn % UBITS))
}

// ---------------------------------------------------------------------------
// internal variables, bundled for easy passing around
// ---------------------------------------------------------------------------

#[repr(C)]
struct vars {
    re: *mut regex_t,
    g: *mut guts,
    /// copies of arguments
    eflags: c_int,
    nmatch: Size,
    pmatch: *mut regmatch_t,
    details: *mut rm_detail_t,
    /// start of string
    start: *mut chr,
    /// search start of string
    search_start: *mut chr,
    /// just past end of string
    stop: *mut chr,
    /// error code if any (0 none)
    err: c_int,
    /// per-tree-subre DFAs
    subdfas: *mut *mut dfa,
    /// per-lacon-subre DFAs
    ladfas: *mut *mut dfa,
    /// per-lacon-subre lookbehind restart data
    lblastcss: *mut *mut sset,
    /// per-lacon-subre lookbehind restart data
    lblastcp: *mut *mut chr,
    dfa1: smalldfa,
    dfa2: smalldfa,
}

// #define VISERR(vv) ((vv)->err != 0) -- have we seen an error yet?
#[inline]
unsafe fn VISERR(vv: *mut vars) -> bool {
    (*vv).err != 0
}

// #define VERR(vv,e) ((vv)->err = ((vv)->err ? (vv)->err : (e)))
#[inline]
unsafe fn VERR(vv: *mut vars, e: c_int) -> c_int {
    (*vv).err = if (*vv).err != 0 { (*vv).err } else { e };
    (*vv).err
}

// #define OFF(p) ((p) - v->start)
#[inline]
unsafe fn OFF(v: *mut vars, p: *mut chr) -> Size {
    (p as isize - (*v).start as isize) as usize / core::mem::size_of::<chr>()
}

// ---------------------------------------------------------------------------
// pg_regexec - match regular expression
// ---------------------------------------------------------------------------

pub unsafe fn pg_regexec(
    re: *mut regex_t,
    string: *const chr,
    len: Size,
    search_start: Size,
    details: *mut rm_detail_t,
    nmatch: Size,
    pmatch: *mut regmatch_t,
    flags: c_int,
) -> c_int {
    let mut var: vars = core::mem::zeroed();
    let v: *mut vars = &mut var;
    let st: c_int;
    let mut n: Size;
    let mut i: Size;
    let backref: c_int;

    const LOCALMAT: usize = 20;
    let mut mat: [regmatch_t; LOCALMAT] = core::mem::zeroed();

    const LOCALDFAS: usize = 40;
    let mut subdfas: [*mut dfa; LOCALDFAS] = [std::ptr::null_mut(); LOCALDFAS];

    /* sanity checks */
    if re.is_null() || string.is_null() || (*re).re_magic != REMAGIC {
        return REG_INVARG;
    }
    if (*re).re_csize != core::mem::size_of::<chr>() as c_int {
        return REG_MIXED;
    }
    if search_start > len {
        return REG_NOMATCH;
    }

    /* Initialize locale-dependent support */
    pg_set_regex_collation((*re).re_collation);

    /* setup */
    (*v).re = re;
    (*v).g = (*re).re_guts as *mut guts;
    if ((*(*v).g).cflags & REG_EXPECT) != 0 && details.is_null() {
        return REG_INVARG;
    }
    if ((*(*v).g).info & REG_UIMPOSSIBLE) != 0 {
        return REG_NOMATCH;
    }
    backref = if ((*(*v).g).info & REG_UBACKREF) != 0 { 1 } else { 0 };
    (*v).eflags = flags;
    let mut nmatch = nmatch;
    if backref != 0 && nmatch <= (*(*v).g).nsub {
        /* need larger work area */
        (*v).nmatch = (*(*v).g).nsub + 1;
        if (*v).nmatch <= LOCALMAT {
            (*v).pmatch = mat.as_mut_ptr();
        } else {
            (*v).pmatch =
                MALLOC((*v).nmatch * core::mem::size_of::<regmatch_t>()) as *mut regmatch_t;
        }
        if (*v).pmatch.is_null() {
            return REG_ESPACE;
        }
        zapallsubs((*v).pmatch, (*v).nmatch);
    } else {
        /* we can store results directly in caller's array */
        (*v).pmatch = pmatch;
        /* ensure any extra entries in caller's array are filled with -1 */
        if nmatch > 0 {
            zapallsubs(pmatch, nmatch);
        }
        /* then forget about extra entries, to avoid useless work in find() */
        if nmatch > (*(*v).g).nsub + 1 {
            nmatch = (*(*v).g).nsub + 1;
        }
        (*v).nmatch = nmatch;
    }
    (*v).details = details;
    (*v).start = string as *mut chr;
    (*v).search_start = (string as *mut chr).add(search_start);
    (*v).stop = (string as *mut chr).add(len);
    (*v).err = 0;
    (*v).subdfas = std::ptr::null_mut();
    (*v).ladfas = std::ptr::null_mut();
    (*v).lblastcss = std::ptr::null_mut();
    (*v).lblastcp = std::ptr::null_mut();
    /* below this point, "goto cleanup" will behave sanely */

    Assert!((*(*v).g).ntree >= 0);

    // C uses "goto cleanup" for the error exits below.  We emulate it with a
    // labeled block: breaking out of 'setup runs the cleanup code, with st
    // already set to the desired return code.
    'setup: {
        n = (*(*v).g).ntree as Size;
        if n <= LOCALDFAS {
            (*v).subdfas = subdfas.as_mut_ptr();
        } else {
            (*v).subdfas = MALLOC(n * core::mem::size_of::<*mut dfa>()) as *mut *mut dfa;
            if (*v).subdfas.is_null() {
                st = REG_ESPACE;
                break 'setup;
            }
        }
        i = 0;
        while i < n {
            *(*v).subdfas.add(i) = std::ptr::null_mut();
            i += 1;
        }

        Assert!((*(*v).g).nlacons >= 0);
        n = (*(*v).g).nlacons as Size;
        if n > 0 {
            (*v).ladfas = MALLOC(n * core::mem::size_of::<*mut dfa>()) as *mut *mut dfa;
            if (*v).ladfas.is_null() {
                st = REG_ESPACE;
                break 'setup;
            }
            i = 0;
            while i < n {
                *(*v).ladfas.add(i) = std::ptr::null_mut();
                i += 1;
            }
            (*v).lblastcss = MALLOC(n * core::mem::size_of::<*mut sset>()) as *mut *mut sset;
            (*v).lblastcp = MALLOC(n * core::mem::size_of::<*mut chr>()) as *mut *mut chr;
            if (*v).lblastcss.is_null() || (*v).lblastcp.is_null() {
                st = REG_ESPACE;
                break 'setup;
            }
            i = 0;
            while i < n {
                *(*v).lblastcss.add(i) = std::ptr::null_mut();
                *(*v).lblastcp.add(i) = std::ptr::null_mut();
                i += 1;
            }
        }

        /* do it */
        Assert!(!(*(*v).g).tree.is_null());
        if backref != 0 {
            st = cfind(v, &mut (*(*(*v).g).tree).cnfa, &mut (*(*v).g).cmap);
        } else {
            st = find(v, &mut (*(*(*v).g).tree).cnfa, &mut (*(*v).g).cmap);
        }

        /* on success, ensure caller's match vector is filled correctly */
        if st == REG_OKAY && nmatch > 0 {
            if (*v).pmatch != pmatch {
                /* copy portion of match vector over from (larger) work area */
                Assert!(nmatch <= (*v).nmatch);
                std::ptr::copy_nonoverlapping((*v).pmatch, pmatch, nmatch);
            }
            if ((*(*v).g).cflags & REG_NOSUB) != 0 {
                /* don't expose possibly-partial sub-match results to caller */
                zapallsubs(pmatch, nmatch);
            }
        }
    } // 'setup (cleanup falls through below)

    /* clean up */
    // cleanup:
    if (*v).pmatch != pmatch && (*v).pmatch != mat.as_mut_ptr() {
        FREE((*v).pmatch as *mut c_void);
    }
    if !(*v).subdfas.is_null() {
        n = (*(*v).g).ntree as Size;
        i = 0;
        while i < n {
            if !(*(*v).subdfas.add(i)).is_null() {
                freedfa(*(*v).subdfas.add(i));
            }
            i += 1;
        }
        if (*v).subdfas != subdfas.as_mut_ptr() {
            FREE((*v).subdfas as *mut c_void);
        }
    }
    if !(*v).ladfas.is_null() {
        n = (*(*v).g).nlacons as Size;
        i = 0;
        while i < n {
            if !(*(*v).ladfas.add(i)).is_null() {
                freedfa(*(*v).ladfas.add(i));
            }
            i += 1;
        }
        FREE((*v).ladfas as *mut c_void);
    }
    if !(*v).lblastcss.is_null() {
        FREE((*v).lblastcss as *mut c_void);
    }
    if !(*v).lblastcp.is_null() {
        FREE((*v).lblastcp as *mut c_void);
    }

    st
}

// ---------------------------------------------------------------------------
// getsubdfa - create or re-fetch the DFA for a tree subre node
//
// We only need to create the DFA once per overall regex execution.
// The DFA will be freed by the cleanup step in pg_regexec().
// ---------------------------------------------------------------------------

unsafe fn getsubdfa(v: *mut vars, t: *mut subre) -> *mut dfa {
    let mut d: *mut dfa = *(*v).subdfas.add((*t).id as usize);

    if d.is_null() {
        d = newdfa(v, &mut (*t).cnfa, &mut (*(*v).g).cmap, DOMALLOC);
        if d.is_null() {
            return std::ptr::null_mut();
        }
        /* set up additional info if this is a backref node */
        if (*t).op == b'b' as c_char {
            (*d).backno = (*t).backno;
            (*d).backmin = (*t).min;
            (*d).backmax = (*t).max;
        }
        *(*v).subdfas.add((*t).id as usize) = d;
    }
    d
}

// ---------------------------------------------------------------------------
// getladfa - create or re-fetch the DFA for a LACON subre node
//
// Same as above, but for LACONs.
// ---------------------------------------------------------------------------

unsafe fn getladfa(v: *mut vars, n: c_int) -> *mut dfa {
    Assert!(n > 0 && n < (*(*v).g).nlacons && !(*(*v).g).lacons.is_null());

    if (*(*v).ladfas.add(n as usize)).is_null() {
        let sub: *mut subre = (*(*v).g).lacons.add(n as usize);

        *(*v).ladfas.add(n as usize) = newdfa(v, &mut (*sub).cnfa, &mut (*(*v).g).cmap, DOMALLOC);
        /* a LACON can't contain a backref, so nothing else to do */
    }
    *(*v).ladfas.add(n as usize)
}

// ---------------------------------------------------------------------------
// find - find a match for the main NFA (no-complications case)
// ---------------------------------------------------------------------------

unsafe fn find(v: *mut vars, cnfa: *mut cnfa, cm: *mut colormap) -> c_int {
    let s: *mut dfa;
    let d: *mut dfa;
    let mut begin: *mut chr;
    let mut end: *mut chr = std::ptr::null_mut();
    let mut cold: *mut chr;
    let open: *mut chr; /* open and close of range of possible starts */
    let close: *mut chr;
    let mut hitend: c_int = 0;
    let shorter = if ((*(*(*v).g).tree).flags & SHORTER) != 0 { 1 } else { 0 };

    /* first, a shot with the search RE */
    s = newdfa(v, &mut (*(*v).g).search, cm, &mut (*v).dfa1);
    if s.is_null() {
        return (*v).err;
    }
    // MDEBUG(("\nsearch at %ld\n", LOFF(v->start)));
    cold = std::ptr::null_mut();
    close = shortest(
        v,
        s,
        (*v).search_start,
        (*v).search_start,
        (*v).stop,
        &mut cold,
        std::ptr::null_mut(),
    );
    freedfa(s);
    if VISERR(v) {
        return (*v).err;
    }
    if ((*(*v).g).cflags & REG_EXPECT) != 0 {
        Assert!(!(*v).details.is_null());
        if !cold.is_null() {
            (*(*v).details).rm_extend.rm_so = OFF(v, cold) as crate::regex::regex::regoff_t;
        } else {
            (*(*v).details).rm_extend.rm_so = OFF(v, (*v).stop) as crate::regex::regex::regoff_t;
        }
        (*(*v).details).rm_extend.rm_eo = OFF(v, (*v).stop) as crate::regex::regex::regoff_t; /* unknown */
    }
    if close.is_null() {
        /* not found */
        return REG_NOMATCH;
    }
    if (*v).nmatch == 0 {
        /* found, don't need exact location */
        return REG_OKAY;
    }

    /* find starting point and match */
    Assert!(!cold.is_null());
    open = cold;
    cold = std::ptr::null_mut();
    // MDEBUG(("between %ld and %ld\n", LOFF(open), LOFF(close)));
    d = newdfa(v, cnfa, cm, &mut (*v).dfa1);
    if d.is_null() {
        return (*v).err;
    }
    begin = open;
    while begin <= close {
        // MDEBUG(("\nfind trying at %ld\n", LOFF(begin)));
        if shorter != 0 {
            end = shortest(v, d, begin, begin, (*v).stop, std::ptr::null_mut(), &mut hitend);
        } else {
            end = longest(v, d, begin, (*v).stop, &mut hitend);
        }
        if VISERR(v) {
            freedfa(d);
            return (*v).err;
        }
        if hitend != 0 && cold.is_null() {
            cold = begin;
        }
        if !end.is_null() {
            break; /* NOTE BREAK OUT */
        }
        begin = begin.add(1);
    }
    Assert!(!end.is_null()); /* search RE succeeded so loop should */
    freedfa(d);

    /* and pin down details */
    Assert!((*v).nmatch > 0);
    (*(*v).pmatch.add(0)).rm_so = OFF(v, begin) as crate::regex::regex::regoff_t;
    (*(*v).pmatch.add(0)).rm_eo = OFF(v, end) as crate::regex::regex::regoff_t;
    if ((*(*v).g).cflags & REG_EXPECT) != 0 {
        if !cold.is_null() {
            (*(*v).details).rm_extend.rm_so = OFF(v, cold) as crate::regex::regex::regoff_t;
        } else {
            (*(*v).details).rm_extend.rm_so = OFF(v, (*v).stop) as crate::regex::regex::regoff_t;
        }
        (*(*v).details).rm_extend.rm_eo = OFF(v, (*v).stop) as crate::regex::regex::regoff_t; /* unknown */
    }
    if (*v).nmatch == 1 {
        /* no need for submatches */
        return REG_OKAY;
    }

    /* find submatches */
    cdissect(v, (*(*v).g).tree, begin, end)
}

// ---------------------------------------------------------------------------
// cfind - find a match for the main NFA (with complications)
// ---------------------------------------------------------------------------

unsafe fn cfind(v: *mut vars, cnfa: *mut cnfa, cm: *mut colormap) -> c_int {
    let s: *mut dfa;
    let d: *mut dfa;
    let mut cold: *mut chr = std::ptr::null_mut();
    let ret: c_int;

    s = newdfa(v, &mut (*(*v).g).search, cm, &mut (*v).dfa1);
    if s.is_null() {
        return (*v).err;
    }
    d = newdfa(v, cnfa, cm, &mut (*v).dfa2);
    if d.is_null() {
        freedfa(s);
        return (*v).err;
    }

    ret = cfindloop(v, cnfa, cm, d, s, &mut cold);

    freedfa(d);
    freedfa(s);
    if VISERR(v) {
        return (*v).err;
    }
    if ((*(*v).g).cflags & REG_EXPECT) != 0 {
        Assert!(!(*v).details.is_null());
        if !cold.is_null() {
            (*(*v).details).rm_extend.rm_so = OFF(v, cold) as crate::regex::regex::regoff_t;
        } else {
            (*(*v).details).rm_extend.rm_so = OFF(v, (*v).stop) as crate::regex::regex::regoff_t;
        }
        (*(*v).details).rm_extend.rm_eo = OFF(v, (*v).stop) as crate::regex::regex::regoff_t; /* unknown */
    }
    ret
}

// ---------------------------------------------------------------------------
// cfindloop - the heart of cfind
// ---------------------------------------------------------------------------

unsafe fn cfindloop(
    v: *mut vars,
    _cnfa: *mut cnfa,
    _cm: *mut colormap,
    d: *mut dfa,
    s: *mut dfa,
    coldp: *mut *mut chr, /* where to put coldstart pointer */
) -> c_int {
    let mut begin: *mut chr;
    let mut end: *mut chr;
    let mut cold: *mut chr;
    let mut open: *mut chr; /* open and close of range of possible starts */
    let mut close: *mut chr;
    let mut estart: *mut chr;
    let mut estop: *mut chr;
    let mut er: c_int;
    let shorter = (*(*(*v).g).tree).flags as c_int & SHORTER;
    let mut hitend: c_int = 0;

    Assert!(!d.is_null() && !s.is_null());
    cold = std::ptr::null_mut();
    close = (*v).search_start;
    loop {
        /* Search with the search RE for match range at/beyond "close" */
        // MDEBUG(("\ncsearch at %ld\n", LOFF(close)));
        close = shortest(v, s, close, close, (*v).stop, &mut cold, std::ptr::null_mut());
        if VISERR(v) {
            *coldp = cold;
            return (*v).err;
        }
        if close.is_null() {
            break; /* no more possible match anywhere */
        }
        Assert!(!cold.is_null());
        open = cold;
        cold = std::ptr::null_mut();
        /* Search for matches starting between "open" and "close" inclusive */
        // MDEBUG(("cbetween %ld and %ld\n", LOFF(open), LOFF(close)));
        begin = open;
        while begin <= close {
            // MDEBUG(("\ncfind trying at %ld\n", LOFF(begin)));
            estart = begin;
            estop = (*v).stop;
            loop {
                /* Here we use the top node's detailed RE */
                if shorter != 0 {
                    end = shortest(v, d, begin, estart, estop, std::ptr::null_mut(), &mut hitend);
                } else {
                    end = longest(v, d, begin, estop, &mut hitend);
                }
                if VISERR(v) {
                    *coldp = cold;
                    return (*v).err;
                }
                if hitend != 0 && cold.is_null() {
                    cold = begin;
                }
                if end.is_null() {
                    break; /* no match with this begin point, try next */
                }
                // MDEBUG(("tentative end %ld\n", LOFF(end)));
                /* Dissect the potential match to see if it really matches */
                er = cdissect(v, (*(*v).g).tree, begin, end);
                if er == REG_OKAY {
                    if (*v).nmatch > 0 {
                        (*(*v).pmatch.add(0)).rm_so = OFF(v, begin) as regoff_t;
                        (*(*v).pmatch.add(0)).rm_eo = OFF(v, end) as regoff_t;
                    }
                    *coldp = cold;
                    return REG_OKAY;
                }
                if er != REG_NOMATCH {
                    VERR(v, er);
                    *coldp = cold;
                    return er;
                }
                /* Try next longer/shorter match with same begin point */
                if shorter != 0 {
                    if end == estop {
                        break; /* no more, so try next begin point */
                    }
                    estart = end.add(1);
                } else {
                    if end == begin {
                        break; /* no more, so try next begin point */
                    }
                    estop = end.sub(1);
                }
            } /* end loop over endpoint positions */
            begin = begin.add(1);
        } /* end loop over beginning positions */

        /*
         * If we get here, there is no possible match starting at or before
         * "close", so consider matches beyond that.  We'll do a fresh search
         * with the search RE to find a new promising match range.
         */
        close = close.add(1);
        if !(close < (*v).stop) {
            break;
        }
    }

    *coldp = cold;
    REG_NOMATCH
}

// ---------------------------------------------------------------------------
// zapallsubs - initialize all subexpression matches to "no match"
//
// Note that p[0], the overall-match location, is not touched.
// ---------------------------------------------------------------------------

unsafe fn zapallsubs(p: *mut regmatch_t, n: Size) {
    let mut i: Size = n - 1;
    while i > 0 {
        (*p.add(i)).rm_so = -1;
        (*p.add(i)).rm_eo = -1;
        i -= 1;
    }
}

// ---------------------------------------------------------------------------
// zaptreesubs - initialize subexpressions within subtree to "no match"
// ---------------------------------------------------------------------------

unsafe fn zaptreesubs(v: *mut vars, t: *mut subre) {
    let n = (*t).capno;
    let mut t2: *mut subre;

    if n > 0 {
        if (n as Size) < (*v).nmatch {
            (*(*v).pmatch.add(n as usize)).rm_so = -1;
            (*(*v).pmatch.add(n as usize)).rm_eo = -1;
        }
    }

    t2 = (*t).child;
    while !t2.is_null() {
        zaptreesubs(v, t2);
        t2 = (*t2).sibling;
    }
}

// ---------------------------------------------------------------------------
// subset - set subexpression match data for a successful subre
// ---------------------------------------------------------------------------

unsafe fn subset(v: *mut vars, sub: *mut subre, begin: *mut chr, end: *mut chr) {
    let n = (*sub).capno;

    Assert!(n > 0);
    if (n as Size) >= (*v).nmatch {
        return;
    }

    // MDEBUG(("%d: setting %d = %ld-%ld\n", sub->id, n, LOFF(begin), LOFF(end)));
    (*(*v).pmatch.add(n as usize)).rm_so = OFF(v, begin) as regoff_t;
    (*(*v).pmatch.add(n as usize)).rm_eo = OFF(v, end) as regoff_t;
}

// ---------------------------------------------------------------------------
// cdissect - check backrefs and determine subexpression matches
//
// cdissect recursively processes a subre tree to check matching of backrefs
// and/or identify submatch boundaries for capture nodes.  The proposed match
// runs from "begin" to "end" (not including "end"), and we are basically
// "dissecting" it to see where the submatches are.
//
// (see C source for full rules 1..6)
// ---------------------------------------------------------------------------

unsafe fn cdissect(
    v: *mut vars,
    t: *mut subre,
    begin: *mut chr, /* beginning of relevant substring */
    end: *mut chr,   /* end of same */
) -> c_int {
    let mut er: c_int;

    Assert!(!t.is_null());
    // MDEBUG(("%d: cdissect %c %ld-%ld\n", t->id, t->op, LOFF(begin), LOFF(end)));

    /* handy place to check for operation cancel */
    INTERRUPT((*v).re);
    /* ... and stack overrun */
    if STACK_TOO_DEEP((*(*v).re).re_fns as *mut fns) != 0 {
        return REG_ETOOBIG;
    }

    match (*t).op as u8 {
        b'=' => {
            /* terminal node */
            Assert!((*t).child.is_null());
            er = REG_OKAY; /* no action, parent did the work */
        }
        b'b' => {
            /* back reference */
            Assert!((*t).child.is_null());
            er = cbrdissect(v, t, begin, end);
        }
        b'.' => {
            /* concatenation */
            Assert!(!(*t).child.is_null());
            if (*(*t).child).flags as c_int & SHORTER != 0 {
                /* reverse scan */
                er = crevcondissect(v, t, begin, end);
            } else {
                er = ccondissect(v, t, begin, end);
            }
        }
        b'|' => {
            /* alternation */
            Assert!(!(*t).child.is_null());
            er = caltdissect(v, t, begin, end);
        }
        b'*' => {
            /* iteration */
            Assert!(!(*t).child.is_null());
            if (*(*t).child).flags as c_int & SHORTER != 0 {
                /* reverse scan */
                er = creviterdissect(v, t, begin, end);
            } else {
                er = citerdissect(v, t, begin, end);
            }
        }
        b'(' => {
            /* no-op capture node */
            Assert!(!(*t).child.is_null());
            er = cdissect(v, (*t).child, begin, end);
        }
        _ => {
            er = REG_ASSERT;
        }
    }

    /*
     * We should never have a match failure unless backrefs lurk below;
     * otherwise, either caller failed to check the DFA, or there's some
     * inconsistency between the DFA and the node's innards.
     */
    Assert!(er != REG_NOMATCH || ((*t).flags as c_int & BACKR) != 0);

    /*
     * If this node is marked as capturing, save successful match's location.
     */
    if (*t).capno > 0 && er == REG_OKAY {
        subset(v, t, begin, end);
    }

    er
}

// ---------------------------------------------------------------------------
// ccondissect - dissect match for concatenation node
// ---------------------------------------------------------------------------

unsafe fn ccondissect(
    v: *mut vars,
    t: *mut subre,
    begin: *mut chr, /* beginning of relevant substring */
    end: *mut chr,   /* end of same */
) -> c_int {
    let left: *mut subre = (*t).child;
    let right: *mut subre = (*left).sibling;
    let d: *mut dfa;
    let d2: *mut dfa;
    let mut mid: *mut chr;
    let mut er: c_int;

    Assert!((*t).op == b'.' as c_char);
    Assert!(!left.is_null() && (*left).cnfa.nstates > 0);
    Assert!(!right.is_null() && (*right).cnfa.nstates > 0);
    Assert!((*right).sibling.is_null());
    Assert!((*left).flags as c_int & SHORTER == 0);

    d = getsubdfa(v, left);
    if VISERR(v) {
        return (*v).err;
    }
    d2 = getsubdfa(v, right);
    if VISERR(v) {
        return (*v).err;
    }
    // MDEBUG(("%d: ccondissect %ld-%ld\n", t->id, LOFF(begin), LOFF(end)));

    /* pick a tentative midpoint */
    mid = longest(v, d, begin, end, std::ptr::null_mut());
    if VISERR(v) {
        return (*v).err;
    }
    if mid.is_null() {
        return REG_NOMATCH;
    }
    // MDEBUG(("%d: tentative midpoint %ld\n", t->id, LOFF(mid)));

    /* iterate until satisfaction or failure */
    loop {
        /* try this midpoint on for size */
        if longest(v, d2, mid, end, std::ptr::null_mut()) == end {
            er = cdissect(v, left, begin, mid);
            if er == REG_OKAY {
                er = cdissect(v, right, mid, end);
                if er == REG_OKAY {
                    /* satisfaction */
                    // MDEBUG(("%d: successful\n", t->id));
                    return REG_OKAY;
                }
                /* Reset left's matches (right should have done so itself) */
                zaptreesubs(v, left);
            }
            if er != REG_NOMATCH {
                return er;
            }
        }
        if VISERR(v) {
            return (*v).err;
        }

        /* that midpoint didn't work, find a new one */
        if mid == begin {
            /* all possibilities exhausted */
            // MDEBUG(("%d: no midpoint\n", t->id));
            return REG_NOMATCH;
        }
        mid = longest(v, d, begin, mid.sub(1), std::ptr::null_mut());
        if VISERR(v) {
            return (*v).err;
        }
        if mid.is_null() {
            /* failed to find a new one */
            // MDEBUG(("%d: failed midpoint\n", t->id));
            return REG_NOMATCH;
        }
        // MDEBUG(("%d: new midpoint %ld\n", t->id, LOFF(mid)));
    }
}

// ---------------------------------------------------------------------------
// crevcondissect - dissect match for concatenation node, shortest-first
// ---------------------------------------------------------------------------

unsafe fn crevcondissect(
    v: *mut vars,
    t: *mut subre,
    begin: *mut chr, /* beginning of relevant substring */
    end: *mut chr,   /* end of same */
) -> c_int {
    let left: *mut subre = (*t).child;
    let right: *mut subre = (*left).sibling;
    let d: *mut dfa;
    let d2: *mut dfa;
    let mut mid: *mut chr;
    let mut er: c_int;

    Assert!((*t).op == b'.' as c_char);
    Assert!(!left.is_null() && (*left).cnfa.nstates > 0);
    Assert!(!right.is_null() && (*right).cnfa.nstates > 0);
    Assert!((*right).sibling.is_null());
    Assert!((*left).flags as c_int & SHORTER != 0);

    d = getsubdfa(v, left);
    if VISERR(v) {
        return (*v).err;
    }
    d2 = getsubdfa(v, right);
    if VISERR(v) {
        return (*v).err;
    }
    // MDEBUG(("%d: crevcondissect %ld-%ld\n", t->id, LOFF(begin), LOFF(end)));

    /* pick a tentative midpoint */
    mid = shortest(v, d, begin, begin, end, std::ptr::null_mut(), std::ptr::null_mut());
    if VISERR(v) {
        return (*v).err;
    }
    if mid.is_null() {
        return REG_NOMATCH;
    }
    // MDEBUG(("%d: tentative midpoint %ld\n", t->id, LOFF(mid)));

    /* iterate until satisfaction or failure */
    loop {
        /* try this midpoint on for size */
        if longest(v, d2, mid, end, std::ptr::null_mut()) == end {
            er = cdissect(v, left, begin, mid);
            if er == REG_OKAY {
                er = cdissect(v, right, mid, end);
                if er == REG_OKAY {
                    /* satisfaction */
                    // MDEBUG(("%d: successful\n", t->id));
                    return REG_OKAY;
                }
                /* Reset left's matches (right should have done so itself) */
                zaptreesubs(v, left);
            }
            if er != REG_NOMATCH {
                return er;
            }
        }
        if VISERR(v) {
            return (*v).err;
        }

        /* that midpoint didn't work, find a new one */
        if mid == end {
            /* all possibilities exhausted */
            // MDEBUG(("%d: no midpoint\n", t->id));
            return REG_NOMATCH;
        }
        mid = shortest(v, d, begin, mid.add(1), end, std::ptr::null_mut(), std::ptr::null_mut());
        if VISERR(v) {
            return (*v).err;
        }
        if mid.is_null() {
            /* failed to find a new one */
            // MDEBUG(("%d: failed midpoint\n", t->id));
            return REG_NOMATCH;
        }
        // MDEBUG(("%d: new midpoint %ld\n", t->id, LOFF(mid)));
    }
}

// ---------------------------------------------------------------------------
// cbrdissect - dissect match for backref node
//
// The backref match might already have been verified by dfa_backref(),
// but we don't know that for sure so must check it here.
// ---------------------------------------------------------------------------

unsafe fn cbrdissect(
    v: *mut vars,
    t: *mut subre,
    begin: *mut chr, /* beginning of relevant substring */
    end: *mut chr,   /* end of same */
) -> c_int {
    let n = (*t).backno;
    let mut numreps: Size;
    let tlen: Size;
    let brlen: Size;
    let brstring: *mut chr;
    let mut p: *mut chr;
    let min = (*t).min;
    let max = (*t).max;

    Assert!(!t.is_null());
    Assert!((*t).op == b'b' as c_char);
    Assert!(n >= 0);
    Assert!((n as Size) < (*v).nmatch);

    // MDEBUG(("%d: cbrdissect %d{%d-%d} %ld-%ld\n", ...));

    /* get the backreferenced string */
    if (*(*v).pmatch.add(n as usize)).rm_so == -1 {
        return REG_NOMATCH;
    }
    brstring = (*v).start.offset((*(*v).pmatch.add(n as usize)).rm_so as isize);
    brlen = ((*(*v).pmatch.add(n as usize)).rm_eo - (*(*v).pmatch.add(n as usize)).rm_so) as Size;

    /* special cases for zero-length strings */
    if brlen == 0 {
        /*
         * matches only if target is zero length, but any number of
         * repetitions can be considered to be present
         */
        if begin == end && min <= max {
            // MDEBUG(("%d: backref matched trivially\n", t->id));
            return REG_OKAY;
        }
        return REG_NOMATCH;
    }
    if begin == end {
        /* matches only if zero repetitions are okay */
        if min == 0 {
            // MDEBUG(("%d: backref matched trivially\n", t->id));
            return REG_OKAY;
        }
        return REG_NOMATCH;
    }

    /*
     * check target length to see if it could possibly be an allowed number of
     * repetitions of brstring
     */
    Assert!(end > begin);
    tlen = (end as isize - begin as isize) as usize / core::mem::size_of::<chr>();
    if tlen % brlen != 0 {
        return REG_NOMATCH;
    }
    numreps = tlen / brlen;
    if numreps < min as Size || (numreps > max as Size && max as c_int != DUPINF) {
        return REG_NOMATCH;
    }

    /* okay, compare the actual string contents */
    p = begin;
    while numreps > 0 {
        numreps -= 1;
        if ((*(*v).g).compare.unwrap())(brstring, p, brlen) != 0 {
            return REG_NOMATCH;
        }
        p = p.add(brlen);
    }

    // MDEBUG(("%d: backref matched\n", t->id));
    REG_OKAY
}

// ---------------------------------------------------------------------------
// caltdissect - dissect match for alternation node
// ---------------------------------------------------------------------------

unsafe fn caltdissect(
    v: *mut vars,
    t: *mut subre,
    begin: *mut chr, /* beginning of relevant substring */
    end: *mut chr,   /* end of same */
) -> c_int {
    let d: *mut dfa;
    let mut er: c_int;

    Assert!((*t).op == b'|' as c_char);

    let mut t = (*t).child;
    /* there should be at least 2 alternatives */
    Assert!(!t.is_null() && !(*t).sibling.is_null());

    while !t.is_null() {
        Assert!((*t).cnfa.nstates > 0);

        // MDEBUG(("%d: caltdissect %ld-%ld\n", t->id, LOFF(begin), LOFF(end)));

        d = getsubdfa(v, t);
        if VISERR(v) {
            return (*v).err;
        }
        if longest(v, d, begin, end, std::ptr::null_mut()) == end {
            // MDEBUG(("%d: caltdissect matched\n", t->id));
            er = cdissect(v, t, begin, end);
            if er != REG_NOMATCH {
                return er;
            }
        }
        if VISERR(v) {
            return (*v).err;
        }

        t = (*t).sibling;
    }

    REG_NOMATCH
}

// ---------------------------------------------------------------------------
// citerdissect - dissect match for iteration node
// ---------------------------------------------------------------------------

unsafe fn citerdissect(
    v: *mut vars,
    t: *mut subre,
    begin: *mut chr, /* beginning of relevant substring */
    end: *mut chr,   /* end of same */
) -> c_int {
    let d: *mut dfa;
    let endpts: *mut *mut chr;
    let mut limit: *mut chr;
    let mut min_matches: c_int;
    let mut max_matches: Size;
    let mut nverified: c_int;
    let mut k: c_int;
    let mut i: c_int;
    let mut er: c_int;

    Assert!((*t).op == b'*' as c_char);
    Assert!(!(*t).child.is_null() && (*(*t).child).cnfa.nstates > 0);
    Assert!((*(*t).child).flags as c_int & SHORTER == 0);
    Assert!(begin <= end);

    // MDEBUG(("%d: citerdissect %ld-%ld\n", t->id, LOFF(begin), LOFF(end)));

    /*
     * For the moment, assume the minimum number of matches is 1.  If zero
     * matches are allowed, and the target string is empty, we are allowed to
     * match regardless of the contents of the iter node --- but we would
     * prefer to match once, so that capturing parens get set.  Therefore, we
     * deal with the zero-matches case at the bottom, after failing to find any
     * other way to match.
     */
    min_matches = (*t).min as c_int;
    if min_matches <= 0 {
        min_matches = 1;
    }

    /*
     * We need workspace to track the endpoints of each sub-match.  Normally
     * we consider only nonzero-length sub-matches, so there can be at most
     * end-begin of them.  However, if min is larger than that, we will also
     * consider zero-length sub-matches in order to find enough matches.
     *
     * For convenience, endpts[0] contains the "begin" pointer and we store
     * sub-match endpoints in endpts[1..max_matches].
     */
    max_matches = (end as isize - begin as isize) as usize / core::mem::size_of::<chr>();
    if max_matches > (*t).max as Size && (*t).max as c_int != DUPINF {
        max_matches = (*t).max as Size;
    }
    if max_matches < min_matches as Size {
        max_matches = min_matches as Size;
    }
    endpts = MALLOC((max_matches + 1) * core::mem::size_of::<*mut chr>()) as *mut *mut chr;
    if endpts.is_null() {
        return REG_ESPACE;
    }
    *endpts.add(0) = begin;

    d = getsubdfa(v, (*t).child);
    if VISERR(v) {
        FREE(endpts as *mut c_void);
        return (*v).err;
    }

    /*
     * Our strategy is to first find a set of sub-match endpoints that are
     * valid according to the child node's DFA, and then recursively dissect
     * each sub-match to confirm validity.  If any validity check fails,
     * backtrack that sub-match and try again.  And, when we next try for a
     * validity check, we need not recheck any successfully verified
     * sub-matches that we didn't move the endpoints of.  nverified remembers
     * how many sub-matches are currently known okay.
     */

    /* initialize to consider first sub-match */
    nverified = 0;
    k = 1;
    limit = end;

    /* iterate until satisfaction or failure */
    'outer: while k > 0 {
        // 'backtrack is a labeled block whose break runs the backtrack code
        'body: {
            /* try to find an endpoint for the k'th sub-match */
            *endpts.add(k as usize) =
                longest(v, d, *endpts.add((k - 1) as usize), limit, std::ptr::null_mut());
            if VISERR(v) {
                FREE(endpts as *mut c_void);
                return (*v).err;
            }
            if (*endpts.add(k as usize)).is_null() {
                /* no match possible, so see if we can shorten previous one */
                k -= 1;
                break 'body; // goto backtrack
            }
            // MDEBUG(("%d: working endpoint %d: %ld\n", t->id, k, LOFF(endpts[k])));

            /* k'th sub-match can no longer be considered verified */
            if nverified >= k {
                nverified = k - 1;
            }

            if *endpts.add(k as usize) != end {
                /* haven't reached end yet, try another iteration if allowed */
                if k as Size >= max_matches {
                    /* must try to shorten some previous match */
                    k -= 1;
                    break 'body; // goto backtrack
                }

                /* reject zero-length match unless necessary to achieve min */
                if *endpts.add(k as usize) == *endpts.add((k - 1) as usize)
                    && (k >= min_matches
                        || ((min_matches - k) as isize)
                            < (end as isize - *endpts.add(k as usize) as isize)
                                / core::mem::size_of::<chr>() as isize)
                {
                    break 'body; // goto backtrack
                }

                k += 1;
                limit = end;
                continue 'outer;
            }

            /*
             * We've identified a way to divide the string into k sub-matches
             * that works so far as the child DFA can tell.  If k is an allowed
             * number of matches, start the slow part: recurse to verify each
             * sub-match.  We always have k <= max_matches, needn't check that.
             */
            if k < min_matches {
                break 'body; // goto backtrack
            }

            // MDEBUG(("%d: verifying %d..%d\n", t->id, nverified + 1, k));

            i = nverified + 1;
            while i <= k {
                /* zap any match data from a non-last iteration */
                zaptreesubs(v, (*t).child);
                er = cdissect(
                    v,
                    (*t).child,
                    *endpts.add((i - 1) as usize),
                    *endpts.add(i as usize),
                );
                if er == REG_OKAY {
                    nverified = i;
                    i += 1;
                    continue;
                }
                if er == REG_NOMATCH {
                    break;
                }
                /* oops, something failed */
                FREE(endpts as *mut c_void);
                return er;
            }

            if i > k {
                /* satisfaction */
                // MDEBUG(("%d: successful\n", t->id));
                FREE(endpts as *mut c_void);
                return REG_OKAY;
            }

            /* i'th match failed to verify, so backtrack it */
            k = i;
        } // 'body -> falls through to backtrack

        // backtrack:
        /*
         * Must consider shorter versions of the k'th sub-match.  However,
         * we'll only ask for a zero-length match if necessary.
         */
        while k > 0 {
            let prev_end: *mut chr = *endpts.add((k - 1) as usize);

            if *endpts.add(k as usize) > prev_end {
                limit = (*endpts.add(k as usize)).sub(1);
                if limit > prev_end
                    || (k < min_matches
                        && (min_matches - k) as isize
                            >= (end as isize - prev_end as isize)
                                / core::mem::size_of::<chr>() as isize)
                {
                    /* break out of backtrack loop, continue the outer one */
                    break;
                }
            }
            /* can't shorten k'th sub-match any more, consider previous one */
            k -= 1;
        }
    }

    /* all possibilities exhausted */
    FREE(endpts as *mut c_void);

    /*
     * Now consider the possibility that we can match to a zero-length string
     * by using zero repetitions.
     */
    if (*t).min == 0 && begin == end {
        // MDEBUG(("%d: allowing zero matches\n", t->id));
        return REG_OKAY;
    }

    // MDEBUG(("%d: failed\n", t->id));
    REG_NOMATCH
}

// ---------------------------------------------------------------------------
// creviterdissect - dissect match for iteration node, shortest-first
// ---------------------------------------------------------------------------

unsafe fn creviterdissect(
    v: *mut vars,
    t: *mut subre,
    begin: *mut chr, /* beginning of relevant substring */
    end: *mut chr,   /* end of same */
) -> c_int {
    let d: *mut dfa;
    let endpts: *mut *mut chr;
    let mut limit: *mut chr;
    let mut min_matches: c_int;
    let mut max_matches: Size;
    let mut nverified: c_int;
    let mut k: c_int;
    let mut i: c_int;
    let mut er: c_int;

    Assert!((*t).op == b'*' as c_char);
    Assert!(!(*t).child.is_null() && (*(*t).child).cnfa.nstates > 0);
    Assert!((*(*t).child).flags as c_int & SHORTER != 0);
    Assert!(begin <= end);

    // MDEBUG(("%d: creviterdissect %ld-%ld\n", t->id, LOFF(begin), LOFF(end)));

    /*
     * If zero matches are allowed, and target string is empty, just declare
     * victory.  OTOH, if target string isn't empty, zero matches can't work
     * so we pretend the min is 1.
     */
    min_matches = (*t).min as c_int;
    if min_matches <= 0 {
        if begin == end {
            // MDEBUG(("%d: allowing zero matches\n", t->id));
            return REG_OKAY;
        }
        min_matches = 1;
    }

    /*
     * We need workspace to track the endpoints of each sub-match.  Normally
     * we consider only nonzero-length sub-matches, so there can be at most
     * end-begin of them.  However, if min is larger than that, we will also
     * consider zero-length sub-matches in order to find enough matches.
     *
     * For convenience, endpts[0] contains the "begin" pointer and we store
     * sub-match endpoints in endpts[1..max_matches].
     */
    max_matches = (end as isize - begin as isize) as usize / core::mem::size_of::<chr>();
    if max_matches > (*t).max as Size && (*t).max as c_int != DUPINF {
        max_matches = (*t).max as Size;
    }
    if max_matches < min_matches as Size {
        max_matches = min_matches as Size;
    }
    endpts = MALLOC((max_matches + 1) * core::mem::size_of::<*mut chr>()) as *mut *mut chr;
    if endpts.is_null() {
        return REG_ESPACE;
    }
    *endpts.add(0) = begin;

    d = getsubdfa(v, (*t).child);
    if VISERR(v) {
        FREE(endpts as *mut c_void);
        return (*v).err;
    }

    /*
     * Our strategy is to first find a set of sub-match endpoints that are
     * valid according to the child node's DFA, and then recursively dissect
     * each sub-match to confirm validity.  (see citerdissect for full notes)
     */

    /* initialize to consider first sub-match */
    nverified = 0;
    k = 1;
    limit = begin;

    /* iterate until satisfaction or failure */
    'outer: while k > 0 {
        'body: {
            /* disallow zero-length match unless necessary to achieve min */
            if limit == *endpts.add((k - 1) as usize)
                && limit != end
                && (k >= min_matches
                    || ((min_matches - k) as isize)
                        < (end as isize - limit as isize) / core::mem::size_of::<chr>() as isize)
            {
                limit = limit.add(1);
            }

            /* if this is the last allowed sub-match, it must reach to the end */
            if k as Size >= max_matches {
                limit = end;
            }

            /* try to find an endpoint for the k'th sub-match */
            *endpts.add(k as usize) = shortest(
                v,
                d,
                *endpts.add((k - 1) as usize),
                limit,
                end,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            if VISERR(v) {
                FREE(endpts as *mut c_void);
                return (*v).err;
            }
            if (*endpts.add(k as usize)).is_null() {
                /* no match possible, so see if we can lengthen previous one */
                k -= 1;
                break 'body; // goto backtrack
            }
            // MDEBUG(("%d: working endpoint %d: %ld\n", t->id, k, LOFF(endpts[k])));

            /* k'th sub-match can no longer be considered verified */
            if nverified >= k {
                nverified = k - 1;
            }

            if *endpts.add(k as usize) != end {
                /* haven't reached end yet, try another iteration if allowed */
                if k as Size >= max_matches {
                    /* must try to lengthen some previous match */
                    k -= 1;
                    break 'body; // goto backtrack
                }

                k += 1;
                limit = *endpts.add((k - 1) as usize);
                continue 'outer;
            }

            /*
             * We've identified a way to divide the string into k sub-matches
             * that works so far as the child DFA can tell.  If k is an allowed
             * number of matches, start the slow part: recurse to verify each
             * sub-match.  We always have k <= max_matches, needn't check that.
             */
            if k < min_matches {
                break 'body; // goto backtrack
            }

            // MDEBUG(("%d: verifying %d..%d\n", t->id, nverified + 1, k));

            i = nverified + 1;
            while i <= k {
                /* zap any match data from a non-last iteration */
                zaptreesubs(v, (*t).child);
                er = cdissect(
                    v,
                    (*t).child,
                    *endpts.add((i - 1) as usize),
                    *endpts.add(i as usize),
                );
                if er == REG_OKAY {
                    nverified = i;
                    i += 1;
                    continue;
                }
                if er == REG_NOMATCH {
                    break;
                }
                /* oops, something failed */
                FREE(endpts as *mut c_void);
                return er;
            }

            if i > k {
                /* satisfaction */
                // MDEBUG(("%d: successful\n", t->id));
                FREE(endpts as *mut c_void);
                return REG_OKAY;
            }

            /* i'th match failed to verify, so backtrack it */
            k = i;
        } // 'body -> falls through to backtrack

        // backtrack:
        /*
         * Must consider longer versions of the k'th sub-match.
         */
        while k > 0 {
            if *endpts.add(k as usize) < end {
                limit = (*endpts.add(k as usize)).add(1);
                /* break out of backtrack loop, continue the outer one */
                break;
            }
            /* can't lengthen k'th sub-match any more, consider previous one */
            k -= 1;
        }
    }

    /* all possibilities exhausted */
    // MDEBUG(("%d: failed\n", t->id));
    FREE(endpts as *mut c_void);
    REG_NOMATCH
}

// ===========================================================================
// DFA routines (rege_dfa.c, #included by regexec.c in the C build)
// ===========================================================================

// ---------------------------------------------------------------------------
// longest - longest-preferred matching engine
//
// On success, returns match endpoint address.  Returns NULL on no match.
// Internal errors also return NULL, with v->err set.
// ---------------------------------------------------------------------------

unsafe fn longest(
    v: *mut vars,
    d: *mut dfa,
    start: *mut chr, /* where the match should start */
    stop: *mut chr,  /* match must end at or before here */
    hitstopp: *mut c_int, /* record whether hit v->stop, if non-NULL */
) -> *mut chr {
    let mut cp: *mut chr;
    let realstop: *mut chr = if stop == (*v).stop { stop } else { stop.add(1) };
    let mut co: color;
    let mut css: *mut sset;
    let mut ss: *mut sset;
    let mut post: *mut chr;
    let mut i: c_int;
    let cm: *mut colormap = (*d).cm;

    /* prevent "uninitialized variable" warnings */
    if !hitstopp.is_null() {
        *hitstopp = 0;
    }

    /* if this is a backref to a known string, just match against that */
    if (*d).backno >= 0 {
        Assert!(((*d).backno as Size) < (*v).nmatch);
        if (*(*v).pmatch.add((*d).backno as usize)).rm_so >= 0 {
            cp = dfa_backref(v, d, start, start, stop, false);
            if cp == (*v).stop && stop == (*v).stop && !hitstopp.is_null() {
                *hitstopp = 1;
            }
            return cp;
        }
    }

    /* fast path for matchall NFAs */
    if (*(*d).cnfa).flags & MATCHALL != 0 {
        let nchr: Size = (stop as isize - start as isize) as usize / core::mem::size_of::<chr>();
        let maxmatchall: Size = (*(*d).cnfa).maxmatchall as Size;

        if (nchr as c_int) < (*(*d).cnfa).minmatchall {
            return std::ptr::null_mut();
        }
        if (*(*d).cnfa).maxmatchall == DUPINF {
            if stop == (*v).stop && !hitstopp.is_null() {
                *hitstopp = 1;
            }
        } else {
            if stop == (*v).stop && nchr <= maxmatchall + 1 && !hitstopp.is_null() {
                *hitstopp = 1;
            }
            if nchr > maxmatchall {
                return start.add(maxmatchall);
            }
        }
        return stop;
    }

    /* initialize */
    css = initialize(v, d, start);
    if css.is_null() {
        return std::ptr::null_mut();
    }
    cp = start;

    /* startup */
    // FDEBUG(("+++ startup +++\n"));
    if cp == (*v).start {
        co = (*(*d).cnfa).bos[if (*v).eflags & REG_NOTBOL != 0 { 0 } else { 1 }];
    } else {
        co = GETCOLOR(cm, *cp.sub(1));
    }
    css = miss(v, d, css, co, cp, start);
    if css.is_null() {
        return std::ptr::null_mut();
    }
    (*css).lastseen = cp;

    /* main text-scanning loop */
    while cp < realstop {
        co = GETCOLOR(cm, *cp);
        ss = *(*css).outs.add(co as usize);
        if ss.is_null() {
            ss = miss(v, d, css, co, cp.add(1), start);
            if ss.is_null() {
                break; /* NOTE BREAK OUT */
            }
        }
        cp = cp.add(1);
        (*ss).lastseen = cp;
        css = ss;
    }

    if VISERR(v) {
        return std::ptr::null_mut();
    }

    /* shutdown */
    // FDEBUG(("+++ shutdown at c%d +++\n", ...));
    if cp == (*v).stop && stop == (*v).stop {
        if !hitstopp.is_null() {
            *hitstopp = 1;
        }
        co = (*(*d).cnfa).eos[if (*v).eflags & REG_NOTEOL != 0 { 0 } else { 1 }];
        ss = miss(v, d, css, co, cp, start);
        if VISERR(v) {
            return std::ptr::null_mut();
        }
        /* special case:  match ended at eol? */
        if !ss.is_null() && ((*ss).flags & POSTSTATE) != 0 {
            return cp;
        } else if !ss.is_null() {
            (*ss).lastseen = cp; /* to be tidy */
        }
    }

    /* find last match, if any */
    post = (*d).lastpost;
    ss = (*d).ssets;
    i = (*d).nssused;
    while i > 0 {
        if ((*ss).flags & POSTSTATE) != 0
            && post != (*ss).lastseen
            && (post.is_null() || post < (*ss).lastseen)
        {
            post = (*ss).lastseen;
        }
        ss = ss.add(1);
        i -= 1;
    }
    if !post.is_null() {
        /* found one */
        return post.sub(1);
    }

    std::ptr::null_mut()
}

// ---------------------------------------------------------------------------
// shortest - shortest-preferred matching engine
//
// On success, returns match endpoint address.  Returns NULL on no match.
// Internal errors also return NULL, with v->err set.
// ---------------------------------------------------------------------------

unsafe fn shortest(
    v: *mut vars,
    d: *mut dfa,
    start: *mut chr,    /* where the match should start */
    mut min: *mut chr,  /* match must end at or after here */
    max: *mut chr,      /* match must end at or before here */
    coldp: *mut *mut chr, /* store coldstart pointer here, if non-NULL */
    hitstopp: *mut c_int, /* record whether hit v->stop, if non-NULL */
) -> *mut chr {
    let mut cp: *mut chr;
    let realmin: *mut chr = if min == (*v).stop { min } else { min.add(1) };
    let realmax: *mut chr = if max == (*v).stop { max } else { max.add(1) };
    let mut co: color;
    let mut css: *mut sset;
    let mut ss: *mut sset;
    let cm: *mut colormap = (*d).cm;

    /* prevent "uninitialized variable" warnings */
    if !coldp.is_null() {
        *coldp = std::ptr::null_mut();
    }
    if !hitstopp.is_null() {
        *hitstopp = 0;
    }

    /* if this is a backref to a known string, just match against that */
    if (*d).backno >= 0 {
        Assert!(((*d).backno as Size) < (*v).nmatch);
        if (*(*v).pmatch.add((*d).backno as usize)).rm_so >= 0 {
            cp = dfa_backref(v, d, start, min, max, true);
            if !cp.is_null() && !coldp.is_null() {
                *coldp = start;
            }
            /* there is no case where we should set *hitstopp */
            return cp;
        }
    }

    /* fast path for matchall NFAs */
    if (*(*d).cnfa).flags & MATCHALL != 0 {
        let nchr: Size = (min as isize - start as isize) as usize / core::mem::size_of::<chr>();

        if (*(*d).cnfa).maxmatchall != DUPINF && nchr > (*(*d).cnfa).maxmatchall as Size {
            return std::ptr::null_mut();
        }
        if ((max as isize - start as isize) as usize / core::mem::size_of::<chr>())
            < (*(*d).cnfa).minmatchall as Size
        {
            return std::ptr::null_mut();
        }
        if nchr < (*(*d).cnfa).minmatchall as Size {
            min = start.add((*(*d).cnfa).minmatchall as usize);
        }
        if !coldp.is_null() {
            *coldp = start;
        }
        /* there is no case where we should set *hitstopp */
        return min;
    }

    /* initialize */
    css = initialize(v, d, start);
    if css.is_null() {
        return std::ptr::null_mut();
    }
    cp = start;

    /* startup */
    // FDEBUG(("--- startup ---\n"));
    if cp == (*v).start {
        co = (*(*d).cnfa).bos[if (*v).eflags & REG_NOTBOL != 0 { 0 } else { 1 }];
    } else {
        co = GETCOLOR(cm, *cp.sub(1));
    }
    css = miss(v, d, css, co, cp, start);
    if css.is_null() {
        return std::ptr::null_mut();
    }
    (*css).lastseen = cp;
    ss = css;

    /* main text-scanning loop */
    while cp < realmax {
        co = GETCOLOR(cm, *cp);
        ss = *(*css).outs.add(co as usize);
        if ss.is_null() {
            ss = miss(v, d, css, co, cp.add(1), start);
            if ss.is_null() {
                break; /* NOTE BREAK OUT */
            }
        }
        cp = cp.add(1);
        (*ss).lastseen = cp;
        css = ss;
        if ((*ss).flags & POSTSTATE) != 0 && cp >= realmin {
            break; /* NOTE BREAK OUT */
        }
    }

    if ss.is_null() {
        return std::ptr::null_mut();
    }

    if !coldp.is_null() {
        /* report last no-progress state set, if any */
        *coldp = lastcold(v, d);
    }

    if ((*ss).flags & POSTSTATE) != 0 && cp > min {
        Assert!(cp >= realmin);
        cp = cp.sub(1);
    } else if cp == (*v).stop && max == (*v).stop {
        co = (*(*d).cnfa).eos[if (*v).eflags & REG_NOTEOL != 0 { 0 } else { 1 }];
        ss = miss(v, d, css, co, cp, start);
        /* match might have ended at eol */
        if (ss.is_null() || ((*ss).flags & POSTSTATE) == 0) && !hitstopp.is_null() {
            *hitstopp = 1;
        }
    }

    if ss.is_null() || ((*ss).flags & POSTSTATE) == 0 {
        return std::ptr::null_mut();
    }

    cp
}

// ---------------------------------------------------------------------------
// matchuntil - incremental matching engine
//
// This is meant for use with a search-style NFA.  We determine whether a
// match exists starting at v->start and ending at probe.  *lastcss and
// *lastcp must be initialized to NULL before starting a series of calls.
//
// Returns 1 if a match exists, 0 if not.
// Internal errors also return 0, with v->err set.
// ---------------------------------------------------------------------------

unsafe fn matchuntil(
    v: *mut vars,
    d: *mut dfa,
    probe: *mut chr,         /* we want to know if a match ends here */
    lastcss: *mut *mut sset, /* state storage across calls */
    lastcp: *mut *mut chr,   /* state storage across calls */
) -> c_int {
    let mut cp: *mut chr = *lastcp;
    let mut co: color;
    let mut css: *mut sset = *lastcss;
    let mut ss: *mut sset;
    let cm: *mut colormap = (*d).cm;

    /* fast path for matchall NFAs */
    if (*(*d).cnfa).flags & MATCHALL != 0 {
        let nchr: Size = (probe as isize - (*v).start as isize) as usize / core::mem::size_of::<chr>();

        if (nchr as c_int) < (*(*d).cnfa).minmatchall {
            return 0;
        }
        /* maxmatchall will always be infinity, cf. makesearch() */
        Assert!((*(*d).cnfa).maxmatchall == DUPINF);
        return 1;
    }

    /* initialize and startup, or restart, if necessary */
    if cp.is_null() || cp > probe {
        cp = (*v).start;
        css = initialize(v, d, cp);
        if css.is_null() {
            return 0;
        }

        co = (*(*d).cnfa).bos[if (*v).eflags & REG_NOTBOL != 0 { 0 } else { 1 }];

        css = miss(v, d, css, co, cp, (*v).start);
        if css.is_null() {
            return 0;
        }
        (*css).lastseen = cp;
    } else if css.is_null() {
        /* we previously found that no match is possible beyond *lastcp */
        return 0;
    }
    ss = css;

    /* main text-scanning loop */
    while cp < probe {
        co = GETCOLOR(cm, *cp);
        ss = *(*css).outs.add(co as usize);
        if ss.is_null() {
            ss = miss(v, d, css, co, cp.add(1), (*v).start);
            if ss.is_null() {
                break; /* NOTE BREAK OUT */
            }
        }
        cp = cp.add(1);
        (*ss).lastseen = cp;
        css = ss;
    }

    *lastcss = ss;
    *lastcp = cp;

    if ss.is_null() {
        return 0; /* impossible match, or internal error */
    }

    /* We need to process one more chr, or the EOS symbol, to check match */
    if cp < (*v).stop {
        co = GETCOLOR(cm, *cp);
        ss = *(*css).outs.add(co as usize);
        if ss.is_null() {
            ss = miss(v, d, css, co, cp.add(1), (*v).start);
        }
    } else {
        Assert!(cp == (*v).stop);
        co = (*(*d).cnfa).eos[if (*v).eflags & REG_NOTEOL != 0 { 0 } else { 1 }];
        ss = miss(v, d, css, co, cp, (*v).start);
    }

    if ss.is_null() || ((*ss).flags & POSTSTATE) == 0 {
        return 0;
    }

    1
}

// ---------------------------------------------------------------------------
// dfa_backref - find best match length for a known backref string
//
// Return match endpoint for longest or shortest valid repeated match,
// or NULL if there is no valid match.  Should be in sync with cbrdissect().
// ---------------------------------------------------------------------------

unsafe fn dfa_backref(
    v: *mut vars,
    d: *mut dfa,
    start: *mut chr, /* where the match should start */
    min: *mut chr,   /* match must end at or after here */
    max: *mut chr,   /* match must end at or before here */
    shortest: bool,
) -> *mut chr {
    let n = (*d).backno;
    let backmin = (*d).backmin;
    let backmax = (*d).backmax;
    let mut numreps: Size;
    let mut minreps: Size;
    let mut maxreps: Size;
    let brlen: Size;
    let brstring: *mut chr;
    let mut p: *mut chr;

    /* get the backreferenced string (caller should have checked this) */
    if (*(*v).pmatch.add(n as usize)).rm_so == -1 {
        return std::ptr::null_mut();
    }
    brstring = (*v).start.offset((*(*v).pmatch.add(n as usize)).rm_so as isize);
    brlen = ((*(*v).pmatch.add(n as usize)).rm_eo - (*(*v).pmatch.add(n as usize)).rm_so) as Size;

    /* special-case zero-length backreference to avoid divide by zero */
    if brlen == 0 {
        /*
         * matches only a zero-length string, but any number of repetitions
         * can be considered to be present
         */
        if min == start && backmin <= backmax {
            return start;
        }
        return std::ptr::null_mut();
    }

    /*
     * convert min and max into numbers of possible repetitions of the backref
     * string, rounding appropriately
     */
    if min <= start {
        minreps = 0;
    } else {
        minreps = ((min as isize - start as isize) as usize / core::mem::size_of::<chr>() - 1)
            / brlen
            + 1;
    }
    maxreps = (max as isize - start as isize) as usize / core::mem::size_of::<chr>() / brlen;

    /* apply bounds, then see if there is any allowed match length */
    if minreps < backmin as Size {
        minreps = backmin as Size;
    }
    if backmax as c_int != DUPINF && maxreps > backmax as Size {
        maxreps = backmax as Size;
    }
    if maxreps < minreps {
        return std::ptr::null_mut();
    }

    /* quick exit if zero-repetitions match is valid and preferred */
    if shortest && minreps == 0 {
        return start;
    }

    /* okay, compare the actual string contents */
    p = start;
    numreps = 0;
    while numreps < maxreps {
        if ((*(*v).g).compare.unwrap())(brstring, p, brlen) != 0 {
            break;
        }
        p = p.add(brlen);
        numreps += 1;
        if shortest && numreps >= minreps {
            break;
        }
    }

    if numreps >= minreps {
        return p;
    }
    std::ptr::null_mut()
}

// ---------------------------------------------------------------------------
// lastcold - determine last point at which no progress had been made
// ---------------------------------------------------------------------------

unsafe fn lastcold(v: *mut vars, d: *mut dfa) -> *mut chr {
    let mut ss: *mut sset;
    let mut nopr: *mut chr;
    let mut i: c_int;

    nopr = (*d).lastnopr;
    if nopr.is_null() {
        nopr = (*v).start;
    }
    ss = (*d).ssets;
    i = (*d).nssused;
    while i > 0 {
        if ((*ss).flags & NOPROGRESS) != 0 && nopr < (*ss).lastseen {
            nopr = (*ss).lastseen;
        }
        ss = ss.add(1);
        i -= 1;
    }
    nopr
}

// ---------------------------------------------------------------------------
// newdfa - set up a fresh DFA
//
// Returns NULL (and sets v->err) on failure.
// ---------------------------------------------------------------------------

unsafe fn newdfa(
    v: *mut vars,
    cnfa: *mut cnfa,
    cm: *mut colormap,
    mut sml: *mut smalldfa, /* preallocated space, may be NULL */
) -> *mut dfa {
    let d: *mut dfa;
    let nss: Size = ((*cnfa).nstates * 2) as Size;
    let wordsper: c_int = ((*cnfa).nstates + UBITS - 1) / UBITS;
    let mut ismalloced: bool = false;

    Assert!(!cnfa.is_null() && (*cnfa).nstates != 0);

    if nss <= FEWSTATES as Size && (*cnfa).ncolors <= FEWCOLORS {
        Assert!(wordsper == 1);
        if sml.is_null() {
            sml = MALLOC(core::mem::size_of::<smalldfa>()) as *mut smalldfa;
            if sml.is_null() {
                VERR(v, REG_ESPACE);
                return std::ptr::null_mut();
            }
            ismalloced = true;
        }
        d = &mut (*sml).dfa;
        (*d).ssets = (*sml).ssets.as_mut_ptr();
        (*d).statesarea = (*sml).statesarea.as_mut_ptr();
        (*d).work = &mut *(*d).statesarea.add(nss);
        (*d).outsarea = (*sml).outsarea.as_mut_ptr();
        (*d).incarea = (*sml).incarea.as_mut_ptr();
        (*d).ismalloced = ismalloced;
        (*d).arraysmalloced = false; /* not separately allocated, anyway */
    } else {
        d = MALLOC(core::mem::size_of::<dfa>()) as *mut dfa;
        if d.is_null() {
            VERR(v, REG_ESPACE);
            return std::ptr::null_mut();
        }
        (*d).ssets = MALLOC(nss * core::mem::size_of::<sset>()) as *mut sset;
        (*d).statesarea = MALLOC(
            (nss + WORK as Size) * wordsper as Size * core::mem::size_of::<c_uint>(),
        ) as *mut c_uint;
        (*d).work = &mut *(*d).statesarea.add(nss * wordsper as Size);
        (*d).outsarea = MALLOC(
            nss * (*cnfa).ncolors as Size * core::mem::size_of::<*mut sset>(),
        ) as *mut *mut sset;
        (*d).incarea = MALLOC(
            nss * (*cnfa).ncolors as Size * core::mem::size_of::<arcp>(),
        ) as *mut arcp;
        (*d).ismalloced = true;
        (*d).arraysmalloced = true;
        /* now freedfa() will behave sanely */
        if (*d).ssets.is_null()
            || (*d).statesarea.is_null()
            || (*d).outsarea.is_null()
            || (*d).incarea.is_null()
        {
            freedfa(d);
            VERR(v, REG_ESPACE);
            return std::ptr::null_mut();
        }
    }

    (*d).nssets = if (*v).eflags & REG_SMALL != 0 { 7 } else { nss as c_int };
    (*d).nssused = 0;
    (*d).nstates = (*cnfa).nstates;
    (*d).ncolors = (*cnfa).ncolors;
    (*d).wordsper = wordsper;
    (*d).cnfa = cnfa;
    (*d).cm = cm;
    (*d).lastpost = std::ptr::null_mut();
    (*d).lastnopr = std::ptr::null_mut();
    (*d).search = (*d).ssets;
    (*d).backno = -1; /* may be set by caller */
    (*d).backmin = 0;
    (*d).backmax = 0;

    /* initialization of sset fields is done as needed */

    d
}

// ---------------------------------------------------------------------------
// freedfa - free a DFA
// ---------------------------------------------------------------------------

unsafe fn freedfa(d: *mut dfa) {
    if (*d).arraysmalloced {
        if !(*d).ssets.is_null() {
            FREE((*d).ssets as *mut c_void);
        }
        if !(*d).statesarea.is_null() {
            FREE((*d).statesarea as *mut c_void);
        }
        if !(*d).outsarea.is_null() {
            FREE((*d).outsarea as *mut c_void);
        }
        if !(*d).incarea.is_null() {
            FREE((*d).incarea as *mut c_void);
        }
    }

    if (*d).ismalloced {
        FREE(d as *mut c_void);
    }
}

// ---------------------------------------------------------------------------
// hash - construct a hash code for a bitvector
//
// There are probably better ways, but they're more expensive.
// ---------------------------------------------------------------------------

unsafe fn hash(uv: *mut c_uint, n: c_int) -> c_uint {
    let mut i: c_int;
    let mut h: c_uint;

    h = 0;
    i = 0;
    while i < n {
        h ^= *uv.add(i as usize);
        i += 1;
    }
    h
}

// ---------------------------------------------------------------------------
// initialize - hand-craft a cache entry for startup, otherwise get ready
// ---------------------------------------------------------------------------

unsafe fn initialize(v: *mut vars, d: *mut dfa, start: *mut chr) -> *mut sset {
    let ss: *mut sset;
    let mut i: c_int;

    /* is previous one still there? */
    if (*d).nssused > 0 && ((*(*d).ssets.add(0)).flags & STARTER) != 0 {
        ss = (*d).ssets.add(0);
    } else {
        /* no, must (re)build it */
        ss = getvacant(v, d, start, start);
        if ss.is_null() {
            return std::ptr::null_mut();
        }
        i = 0;
        while i < (*d).wordsper {
            *(*ss).states.add(i as usize) = 0;
            i += 1;
        }
        BSET((*ss).states, (*(*d).cnfa).pre);
        (*ss).hash = HASH((*ss).states, (*d).wordsper);
        Assert!((*(*d).cnfa).pre != (*(*d).cnfa).post);
        (*ss).flags = STARTER | LOCKED | NOPROGRESS;
        /* lastseen dealt with below */
    }

    i = 0;
    while i < (*d).nssused {
        (*(*d).ssets.add(i as usize)).lastseen = std::ptr::null_mut();
        i += 1;
    }
    (*ss).lastseen = start; /* maybe untrue, but harmless */
    (*d).lastpost = std::ptr::null_mut();
    (*d).lastnopr = std::ptr::null_mut();
    ss
}

// ---------------------------------------------------------------------------
// miss - handle a stateset cache miss
//
// css is the current stateset, co is the color of the current input character,
// cp points to the character after that (which is where we may need to test
// LACONs).  start does not affect matching behavior but is needed for pickss'
// heuristics about which stateset cache entry to replace.
//
// Ordinarily, returns the address of the next stateset.  Returns NULL if no
// valid NFA states remain, ie we have a certain match failure.  Internal
// errors also return NULL, with v->err set.
// ---------------------------------------------------------------------------

unsafe fn miss(
    v: *mut vars,
    d: *mut dfa,
    css: *mut sset,
    co: color,
    cp: *mut chr,    /* next chr */
    start: *mut chr, /* where the attempt got started */
) -> *mut sset {
    let cnfa: *mut cnfa = (*d).cnfa;
    let mut i: c_int;
    let h: c_uint;
    let mut ca: *mut carc;
    let mut p: *mut sset;
    let ispseudocolor: c_int;
    let mut ispost: c_int;
    let mut noprogress: c_int;
    let mut gotstate: c_int;
    let mut dolacons: c_int;
    let mut sawlacons: c_int;

    /* for convenience, we can be called even if it might not be a miss */
    if !(*(*css).outs.add(co as usize)).is_null() {
        // FDEBUG(("hit\n"));
        return *(*css).outs.add(co as usize);
    }
    // FDEBUG(("miss\n"));

    /*
     * Checking for operation cancel in the inner text search loop seems
     * unduly expensive.  As a compromise, check during cache misses.
     */
    INTERRUPT((*v).re);

    /*
     * What set of states would we end up in after consuming the co character?
     */
    i = 0;
    while i < (*d).wordsper {
        *(*d).work.add(i as usize) = 0; /* build new stateset bitmap in d->work */
        i += 1;
    }
    ispseudocolor = (*(*(*d).cm).cd.add(co as usize)).flags & PSEUDO;
    ispost = 0;
    noprogress = 1;
    gotstate = 0;
    i = 0;
    while i < (*d).nstates {
        if ISBSET((*css).states, i) != 0 {
            ca = *(*cnfa).states.add(i as usize);
            while (*ca).co != COLORLESS {
                if (*ca).co == co || ((*ca).co == RAINBOW && ispseudocolor == 0) {
                    BSET((*d).work, (*ca).to);
                    gotstate = 1;
                    if (*ca).to == (*cnfa).post {
                        ispost = 1;
                    }
                    if (*(*cnfa).stflags.add((*ca).to as usize) as c_int & CNFA_NOPROGRESS) == 0 {
                        noprogress = 0;
                    }
                    // FDEBUG(("%d -> %d\n", i, ca->to));
                }
                ca = ca.add(1);
            }
        }
        i += 1;
    }
    if gotstate == 0 {
        return std::ptr::null_mut(); /* character cannot reach any new state */
    }
    dolacons = (*cnfa).flags & HASLACONS;
    sawlacons = 0;
    /* outer loop handles transitive closure of reachable-by-LACON states */
    while dolacons != 0 {
        dolacons = 0;
        i = 0;
        while i < (*d).nstates {
            if ISBSET((*d).work, i) != 0 {
                ca = *(*cnfa).states.add(i as usize);
                while (*ca).co != COLORLESS {
                    if (*ca).co < (*cnfa).ncolors as color {
                        ca = ca.add(1);
                        continue; /* not a LACON arc */
                    }
                    if ISBSET((*d).work, (*ca).to) != 0 {
                        ca = ca.add(1);
                        continue; /* arc would be a no-op anyway */
                    }
                    sawlacons = 1; /* this LACON affects our result */
                    if lacon(v, cnfa, cp, (*ca).co) == 0 {
                        if VISERR(v) {
                            return std::ptr::null_mut();
                        }
                        ca = ca.add(1);
                        continue; /* LACON arc cannot be traversed */
                    }
                    if VISERR(v) {
                        return std::ptr::null_mut();
                    }
                    BSET((*d).work, (*ca).to);
                    dolacons = 1;
                    if (*ca).to == (*cnfa).post {
                        ispost = 1;
                    }
                    if (*(*cnfa).stflags.add((*ca).to as usize) as c_int & CNFA_NOPROGRESS) == 0 {
                        noprogress = 0;
                    }
                    // FDEBUG(("%d :> %d\n", i, ca->to));
                    ca = ca.add(1);
                }
            }
            i += 1;
        }
    }
    h = HASH((*d).work, (*d).wordsper);

    /* Is this stateset already in the cache? */
    p = (*d).ssets;
    i = (*d).nssused;
    while i > 0 {
        if HIT(h, (*d).work, p, (*d).wordsper) {
            // FDEBUG(("cached c%d\n", ...));
            break; /* NOTE BREAK OUT */
        }
        p = p.add(1);
        i -= 1;
    }
    if i == 0 {
        /* nope, need a new cache entry */
        p = getvacant(v, d, cp, start);
        if p.is_null() {
            return std::ptr::null_mut();
        }
        Assert!(p != css);
        i = 0;
        while i < (*d).wordsper {
            *(*p).states.add(i as usize) = *(*d).work.add(i as usize);
            i += 1;
        }
        (*p).hash = h;
        (*p).flags = if ispost != 0 { POSTSTATE } else { 0 };
        if noprogress != 0 {
            (*p).flags |= NOPROGRESS;
        }
        /* lastseen to be dealt with by caller */
    }

    /*
     * Link new stateset to old, unless a LACON affected the result, in which
     * case we don't create the link.
     */
    if sawlacons == 0 {
        // FDEBUG(("c%d[%d]->c%d\n", ...));
        *(*css).outs.add(co as usize) = p;
        *(*css).inchain.add(co as usize) = (*p).ins;
        (*p).ins.ss = css;
        (*p).ins.co = co;
    }
    p
}

// ---------------------------------------------------------------------------
// lacon - lookaround-constraint checker for miss()
// ---------------------------------------------------------------------------

unsafe fn lacon(
    v: *mut vars,
    pcnfa: *mut cnfa, /* parent cnfa */
    cp: *mut chr,
    co: color, /* "color" of the lookaround constraint */
) -> c_int {
    let n: c_int;
    let sub: *mut subre;
    let d: *mut dfa;
    let end: *mut chr;
    let satisfied: c_int;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP((*(*v).re).re_fns as *mut fns) != 0 {
        VERR(v, REG_ETOOBIG);
        return 0;
    }

    n = co as c_int - (*pcnfa).ncolors;
    Assert!(n > 0 && n < (*(*v).g).nlacons && !(*(*v).g).lacons.is_null());
    // FDEBUG(("=== testing lacon %d\n", n));
    sub = (*(*v).g).lacons.add(n as usize);
    d = getladfa(v, n);
    if d.is_null() {
        return 0;
    }
    if LATYPE_IS_AHEAD((*sub).latype as c_int) != 0 {
        /* used to use longest() here, but shortest() could be much cheaper */
        end = shortest(v, d, cp, cp, (*v).stop, std::ptr::null_mut(), std::ptr::null_mut());
        satisfied = if LATYPE_IS_POS((*sub).latype as c_int) != 0 {
            if !end.is_null() { 1 } else { 0 }
        } else if end.is_null() {
            1
        } else {
            0
        };
    } else {
        /*
         * To avoid doing O(N^2) work when repeatedly testing a lookbehind
         * constraint in an N-character string, we use matchuntil() which can
         * cache the DFA state across calls.
         */
        let mut s = matchuntil(
            v,
            d,
            cp,
            (*v).lblastcss.add(n as usize),
            (*v).lblastcp.add(n as usize),
        );
        if LATYPE_IS_POS((*sub).latype as c_int) == 0 {
            s = if s != 0 { 0 } else { 1 };
        }
        satisfied = s;
    }
    // FDEBUG(("=== lacon %d satisfied %d\n", n, satisfied));
    satisfied
}

// ---------------------------------------------------------------------------
// getvacant - get a vacant state set
//
// This routine clears out the inarcs and outarcs, but does not otherwise
// clear the innards of the state set -- that's up to the caller.
// ---------------------------------------------------------------------------

unsafe fn getvacant(v: *mut vars, d: *mut dfa, cp: *mut chr, start: *mut chr) -> *mut sset {
    let mut i: c_int;
    let ss: *mut sset;
    let mut p: *mut sset;
    let mut ap: arcp;
    let mut co: color;

    ss = pickss(v, d, cp, start);
    if ss.is_null() {
        return std::ptr::null_mut();
    }
    Assert!(((*ss).flags & LOCKED) == 0);

    /* clear out its inarcs, including self-referential ones */
    ap = (*ss).ins;
    loop {
        p = ap.ss;
        if p.is_null() {
            break;
        }
        co = ap.co;
        // FDEBUG(("zapping c%d's %ld outarc\n", ...));
        *(*p).outs.add(co as usize) = std::ptr::null_mut();
        ap = *(*p).inchain.add(co as usize);
        (*(*p).inchain.add(co as usize)).ss = std::ptr::null_mut(); /* paranoia */
    }
    (*ss).ins.ss = std::ptr::null_mut();

    /* take it off the inarc chains of the ssets reached by its outarcs */
    i = 0;
    while i < (*d).ncolors {
        p = *(*ss).outs.add(i as usize);
        Assert!(p != ss); /* not self-referential */
        if p.is_null() {
            i += 1;
            continue; /* NOTE CONTINUE */
        }
        // FDEBUG(("del outarc %d from c%d's in chn\n", ...));
        if (*p).ins.ss == ss && (*p).ins.co == i as color {
            (*p).ins = *(*ss).inchain.add(i as usize);
        } else {
            let mut lastap: arcp = arcp {
                ss: std::ptr::null_mut(),
                co: 0,
            };

            Assert!(!(*p).ins.ss.is_null());
            ap = (*p).ins;
            while !ap.ss.is_null() && !(ap.ss == ss && ap.co == i as color) {
                lastap = ap;
                ap = *(*ap.ss).inchain.add(ap.co as usize);
            }
            Assert!(!ap.ss.is_null());
            *(*lastap.ss).inchain.add(lastap.co as usize) = *(*ss).inchain.add(i as usize);
        }
        *(*ss).outs.add(i as usize) = std::ptr::null_mut();
        (*(*ss).inchain.add(i as usize)).ss = std::ptr::null_mut();
        i += 1;
    }

    /* if ss was a success state, may need to remember location */
    if ((*ss).flags & POSTSTATE) != 0
        && (*ss).lastseen != (*d).lastpost
        && ((*d).lastpost.is_null() || (*d).lastpost < (*ss).lastseen)
    {
        (*d).lastpost = (*ss).lastseen;
    }

    /* likewise for a no-progress state */
    if ((*ss).flags & NOPROGRESS) != 0
        && (*ss).lastseen != (*d).lastnopr
        && ((*d).lastnopr.is_null() || (*d).lastnopr < (*ss).lastseen)
    {
        (*d).lastnopr = (*ss).lastseen;
    }

    ss
}

// ---------------------------------------------------------------------------
// pickss - pick the next stateset to be used
// ---------------------------------------------------------------------------

unsafe fn pickss(v: *mut vars, d: *mut dfa, cp: *mut chr, start: *mut chr) -> *mut sset {
    let mut i: c_int;
    let mut ss: *mut sset;
    let mut end: *mut sset;
    let ancient: *mut chr;

    /* shortcut for cases where cache isn't full */
    if (*d).nssused < (*d).nssets {
        i = (*d).nssused;
        (*d).nssused += 1;
        ss = (*d).ssets.add(i as usize);
        // FDEBUG(("new c%d\n", i));
        /* set up innards */
        (*ss).states = (*d).statesarea.add((i * (*d).wordsper) as usize);
        (*ss).flags = 0;
        (*ss).ins.ss = std::ptr::null_mut();
        (*ss).ins.co = WHITE; /* give it some value */
        (*ss).outs = (*d).outsarea.add((i * (*d).ncolors) as usize);
        (*ss).inchain = (*d).incarea.add((i * (*d).ncolors) as usize);
        i = 0;
        while i < (*d).ncolors {
            *(*ss).outs.add(i as usize) = std::ptr::null_mut();
            (*(*ss).inchain.add(i as usize)).ss = std::ptr::null_mut();
            i += 1;
        }
        return ss;
    }

    /* look for oldest, or old enough anyway */
    if (cp as isize - start as isize) as usize / core::mem::size_of::<chr>()
        > ((*d).nssets * 2 / 3) as usize
    {
        /* oldest 33% are expendable */
        ancient = cp.sub(((*d).nssets * 2 / 3) as usize);
    } else {
        ancient = start;
    }
    ss = (*d).search;
    end = (*d).ssets.add((*d).nssets as usize);
    while ss < end {
        if ((*ss).lastseen.is_null() || (*ss).lastseen < ancient) && ((*ss).flags & LOCKED) == 0 {
            (*d).search = ss.add(1);
            // FDEBUG(("replacing c%d\n", ...));
            return ss;
        }
        ss = ss.add(1);
    }
    ss = (*d).ssets;
    end = (*d).search;
    while ss < end {
        if ((*ss).lastseen.is_null() || (*ss).lastseen < ancient) && ((*ss).flags & LOCKED) == 0 {
            (*d).search = ss.add(1);
            // FDEBUG(("replacing c%d\n", ...));
            return ss;
        }
        ss = ss.add(1);
    }

    /* nobody's old enough?!? -- something's really wrong */
    // FDEBUG(("cannot find victim to replace!\n"));
    VERR(v, REG_ASSERT);
    std::ptr::null_mut()
}
