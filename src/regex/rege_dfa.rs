//! DFA routines
//!
//! Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
//!
//! Development of this software was funded, in part, by Cray Research Inc.,
//! UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
//! Corporation, none of whom are responsible for the results.  The author
//! thanks all of them.
//!
//! Redistribution and use in source and binary forms -- with or without
//! modification -- are permitted for any purpose, provided that
//! redistributions in source form retain this entire copyright notice and
//! indicate the origin and nature of any modifications.
//!
//! src/backend/regex/rege_dfa.c
//!
//! In the C build this file is #included by regexec.c, sharing its private
//! struct/macro definitions.  Here it is translated as a standalone module;
//! the lazy-DFA local types (arcp, sset, dfa, smalldfa, vars) and the bit
//! macros are mirrored from regexec, and the shared NFA/colormap types are
//! imported from regguts.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(unused_assignments)]

use std::ffi::{c_char, c_int, c_uint, c_void};

use crate::c::Size;
use crate::regex::regcustom::chr;
use crate::regex::regex::{regex_t, regmatch_t, rm_detail_t, REG_NOTBOL, REG_NOTEOL, REG_SMALL};
use crate::regex::regerror::{REG_ASSERT, REG_ESPACE, REG_ETOOBIG};
use crate::regex::regguts::{
    carc, cnfa, color, colormap, fns, guts, subre, GETCOLOR, CNFA_NOPROGRESS, COLORLESS, DUPINF,
    HASLACONS, LATYPE_IS_AHEAD, LATYPE_IS_POS, MATCHALL, PSEUDO, RAINBOW, STACK_TOO_DEEP, WHITE,
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
// lazy-DFA representation (private to regexec.c / rege_dfa.c)
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

// ---------------------------------------------------------------------------
// getladfa - lazily build the DFA for lookaround constraint n
//
// Defined in regexec.c; stubbed here as a cross-file dependency.
// ---------------------------------------------------------------------------

unsafe fn getladfa(_v: *mut vars, _n: c_int) -> *mut dfa {
    // TODO(pg-port): getladfa() lives in regexec.c
    unimplemented!("getladfa (regexec.c)")
}

// ---------------------------------------------------------------------------
// longest - longest-preferred matching engine
//
// On success, returns match endpoint address.  Returns NULL on no match.
// Internal errors also return NULL, with v->err set.
// ---------------------------------------------------------------------------

unsafe fn longest(
    v: *mut vars,
    d: *mut dfa,
    start: *mut chr,      /* where the match should start */
    stop: *mut chr,       /* match must end at or before here */
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
    start: *mut chr,      /* where the match should start */
    mut min: *mut chr,    /* match must end at or after here */
    max: *mut chr,        /* match must end at or before here */
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
        let nchr: Size =
            (probe as isize - (*v).start as isize) as usize / core::mem::size_of::<chr>();

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
