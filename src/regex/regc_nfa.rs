//! regex/regc_nfa.c - NFA utilities.
//!
//! This file is #included by regcomp.c. Copyright (c) 1998, 1999 Henry Spencer.
//! See PostgreSQL source for the full license text. Builds, optimizes, and
//! compacts the NFA used by the regex compiler.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use crate::prelude::*;

use core::ffi::c_void;

use crate::port::qsort::pg_qsort;
use crate::regex::regcustom::chr;
use crate::regex::regerror::{REG_ASSERT, REG_ESPACE, REG_ETOOBIG};
use crate::regex::regex::{regex_t, REG_UEMPTYMATCH, REG_UIMPOSSIBLE};
use crate::regex::regguts::{
    arc, arcbatch, cnfa, color, colormap, fns, nfa, state, statebatch, subre, ARCBATCHSIZE,
    CNFA_NOPROGRESS, COLORLESS, DUPINF, FIRSTABSIZE, FIRSTSBSIZE, FREESTATE, HASCANTMATCH,
    HASLACONS, MATCHALL, MAXABSIZE, MAXSBSIZE, NOTREACHED, NULLCNFA, PSEUDO, RAINBOW,
    STATEBATCHSIZE, ZAPCNFA,
};
use crate::regex::regguts::{REG_MAX_COMPILE_SPACE, STACK_TOO_DEEP as guts_STACK_TOO_DEEP};
use crate::utils::palloc::{palloc_extended, pfree, MCXT_ALLOC_NO_OOM};

// ---------------------------------------------------------------------------
// regcustom.h macros (MALLOC/FREE/INTERRUPT) expressed at use sites.
// ---------------------------------------------------------------------------

/// C: #define MALLOC(n) palloc_extended((n), MCXT_ALLOC_NO_OOM)
/// Returns NULL on failure rather than throwing.
unsafe fn MALLOC(n: usize) -> *mut c_void {
    palloc_extended(n, MCXT_ALLOC_NO_OOM)
}

/// C: #define FREE(p) pfree(VS(p))
unsafe fn FREE(p: *mut c_void) {
    pfree(p);
}

/// C: #define INTERRUPT(re) CHECK_FOR_INTERRUPTS()
/// The regex library's INTERRUPT() expands to a cancel check.
unsafe fn INTERRUPT(_re: *mut regex_t) {
    crate::miscadmin::CHECK_FOR_INTERRUPTS();
}

/// regguts.h: #define STACK_TOO_DEEP(re) ((*((struct fns *)(re)->re_fns)->stack_too_deep)())
/// regc_nfa.c calls STACK_TOO_DEEP(nfa->v->re); we route through the nfa to its
/// vars' regex_t and dispatch via the fns table in re_fns.
unsafe fn STACK_TOO_DEEP(nfa: *mut nfa) -> bool {
    let re: *mut regex_t = (*((*nfa).v as *mut vars)).re;
    guts_STACK_TOO_DEEP((*re).re_fns as *mut fns) != 0
}

// ---------------------------------------------------------------------------
// struct vars (defined in regcomp.c, not a header). regc_nfa.c reaches into it
// through nfa->v for ->re, ->err, ->nexttype, and ->spaceused. We materialize a
// faithful layout here; regguts::vars is an opaque c_void, so callers cast.
// TODO(pg-port): unify with regcomp.c's struct vars once that file is ported.
// ---------------------------------------------------------------------------
pub use crate::regex::regcomp::vars;

// ---------------------------------------------------------------------------
// arc type codes (regcomp.c #defines, since regc_nfa.c is #included by it).
// TODO(pg-port): these live in regcomp.c; move there when it is translated.
// ---------------------------------------------------------------------------
/// no token present
pub const EMPTY: c_int = b'n' as c_int;
/// ordinary character
pub const PLAIN: c_int = b'p' as c_int;
/// end of string (used by VERR to halt the lexer)
pub const EOS: c_int = b'e' as c_int;
/// lookaround constraint subRE
pub const LACON: c_int = b'L' as c_int;
/// color-lookahead arc
pub const AHEAD: c_int = b'a' as c_int;
/// color-lookbehind arc
pub const BEHIND: c_int = b'r' as c_int;
/// arc that cannot match anything
pub const CANTMATCH: c_int = b'x' as c_int;

/// CHAR_BIT from <limits.h>
pub const CHAR_BIT: c_int = 8;

// combine() result codes (regcomp.c #defines)
/// destroys arc
pub const INCOMPATIBLE: c_int = 1;
/// constraint satisfied
pub const SATISFIED: c_int = 2;
/// compatible but not satisfied yet
pub const COMPATIBLE: c_int = 3;
/// replace arc's color with constraint color
pub const REPLACEARC: c_int = 4;

/// #define COLORED(a) ((a)->co >= 0 && ((a)->type == PLAIN || AHEAD || BEHIND))
#[inline]
unsafe fn COLORED(a: *const arc) -> bool {
    (*a).co >= 0
        && ((*a).r#type == PLAIN || (*a).r#type == AHEAD || (*a).r#type == BEHIND)
}

// ---------------------------------------------------------------------------
// error/status macros: regc_nfa.c uses NISERR()/NERR() referencing nfa->v;
// regcomp.c provides VISERR/VERR/ISERR/ERR referencing a vars*.
// #define NISERR()  VISERR(nfa->v)
// #define NERR(e)   VERR(nfa->v, (e))
// ---------------------------------------------------------------------------

/// #define VISERR(vv) ((vv)->err != 0)
#[inline]
unsafe fn VISERR(vv: *mut vars) -> bool {
    (*vv).err != 0
}

/// #define VERR(vv,e) ((vv)->nexttype = EOS, (vv)->err = ((vv)->err ? (vv)->err : (e)))
#[inline]
unsafe fn VERR(vv: *mut vars, e: c_int) -> c_int {
    (*vv).nexttype = EOS;
    (*vv).err = if (*vv).err != 0 { (*vv).err } else { e };
    (*vv).err
}

/// #define NISERR() VISERR(nfa->v)
#[inline]
unsafe fn NISERR(nfa: *mut nfa) -> bool {
    VISERR((*nfa).v as *mut vars)
}

/// #define NERR(e) VERR(nfa->v, (e))
#[inline]
unsafe fn NERR(nfa: *mut nfa, e: c_int) -> c_int {
    VERR((*nfa).v as *mut vars, e)
}

/// #define ISERR() VISERR(v) -- here referencing the nfa's vars directly.
#[inline]
unsafe fn ISERR(nfa: *mut nfa) -> bool {
    VISERR((*nfa).v as *mut vars)
}

/// #define ERR(e) VERR(v, e) -- record an error on the nfa's vars.
#[inline]
#[allow(dead_code)]
unsafe fn ERR(nfa: *mut nfa, e: c_int) -> c_int {
    VERR((*nfa).v as *mut vars, e)
}

// Dependencies defined in regc_color.c.
use crate::regex::regc_color::{colorchain, maxcolor, pseudocolor, rainbow, uncolorchain};

// ---------------------------------------------------------------------------
// NFA construction and teardown.
// ---------------------------------------------------------------------------

/*
 * newnfa - set up an NFA
 */
pub unsafe fn newnfa(
    v: *mut vars,
    cm: *mut colormap,
    parent: *mut nfa, /* NULL if primary NFA */
) -> *mut nfa /* the NFA, or NULL */ {
    let nfa: *mut nfa;

    nfa = MALLOC(core::mem::size_of::<crate::regex::regguts::nfa>()) as *mut crate::regex::regguts::nfa;
    if nfa.is_null() {
        // ERR(REG_ESPACE) -- but with no nfa yet we set it directly on v.
        VERR(v, REG_ESPACE);
        return null_mut();
    }

    /* Make the NFA minimally valid, so freenfa() will behave sanely */
    (*nfa).states = null_mut();
    (*nfa).slast = null_mut();
    (*nfa).freestates = null_mut();
    (*nfa).freearcs = null_mut();
    (*nfa).lastsb = null_mut();
    (*nfa).lastab = null_mut();
    (*nfa).lastsbused = 0;
    (*nfa).lastabused = 0;
    (*nfa).nstates = 0;
    (*nfa).cm = cm;
    (*nfa).v = v as *mut crate::regex::regguts::vars;
    (*nfa).bos[0] = COLORLESS;
    (*nfa).bos[1] = COLORLESS;
    (*nfa).eos[0] = COLORLESS;
    (*nfa).eos[1] = COLORLESS;
    (*nfa).flags = 0;
    (*nfa).minmatchall = -1;
    (*nfa).maxmatchall = -1;
    (*nfa).parent = parent; /* Precedes newfstate so parent is valid. */

    /* Create required infrastructure */
    (*nfa).post = newfstate(nfa, b'@' as c_int); /* number 0 */
    (*nfa).pre = newfstate(nfa, b'>' as c_int); /* number 1 */
    (*nfa).init = newstate(nfa); /* may become invalid later */
    (*nfa).r#final = newstate(nfa);
    if ISERR(nfa) {
        freenfa(nfa);
        return null_mut();
    }
    rainbow(nfa, (*nfa).cm, PLAIN, COLORLESS, (*nfa).pre, (*nfa).init);
    newarc(nfa, b'^' as c_int, 1, (*nfa).pre, (*nfa).init);
    newarc(nfa, b'^' as c_int, 0, (*nfa).pre, (*nfa).init);
    rainbow(nfa, (*nfa).cm, PLAIN, COLORLESS, (*nfa).r#final, (*nfa).post);
    newarc(nfa, b'$' as c_int, 1, (*nfa).r#final, (*nfa).post);
    newarc(nfa, b'$' as c_int, 0, (*nfa).r#final, (*nfa).post);

    if ISERR(nfa) {
        freenfa(nfa);
        return null_mut();
    }
    nfa
}

/*
 * freenfa - free an entire NFA
 */
pub unsafe fn freenfa(nfa: *mut nfa) {
    let mut sb: *mut statebatch;
    let mut sbnext: *mut statebatch;
    let mut ab: *mut arcbatch;
    let mut abnext: *mut arcbatch;

    sb = (*nfa).lastsb;
    while !sb.is_null() {
        sbnext = (*sb).next;
        (*((*nfa).v as *mut vars)).spaceused -= STATEBATCHSIZE((*sb).nstates);
        FREE(sb as *mut c_void);
        sb = sbnext;
    }
    (*nfa).lastsb = null_mut();
    ab = (*nfa).lastab;
    while !ab.is_null() {
        abnext = (*ab).next;
        (*((*nfa).v as *mut vars)).spaceused -= ARCBATCHSIZE((*ab).narcs);
        FREE(ab as *mut c_void);
        ab = abnext;
    }
    (*nfa).lastab = null_mut();

    (*nfa).nstates = -1;
    FREE(nfa as *mut c_void);
}

/*
 * newstate - allocate an NFA state, with zero flag value
 */
pub unsafe fn newstate(nfa: *mut nfa) -> *mut state /* NULL on error */ {
    let s: *mut state;

    /*
     * This is a handy place to check for operation cancel during regex
     * compilation, since no code path will go very long without making a new
     * state or arc.
     */
    INTERRUPT((*((*nfa).v as *mut vars)).re);

    /* first, recycle anything that's on the freelist */
    if !(*nfa).freestates.is_null() {
        s = (*nfa).freestates;
        (*nfa).freestates = (*s).next;
    }
    /* otherwise, is there anything left in the last statebatch? */
    else if !(*nfa).lastsb.is_null() && (*nfa).lastsbused < (*(*nfa).lastsb).nstates {
        s = (*(*nfa).lastsb).s.as_mut_ptr().add((*nfa).lastsbused);
        (*nfa).lastsbused += 1;
    }
    /* otherwise, need to allocate a new statebatch */
    else {
        let newSb: *mut statebatch;
        let mut nstates: Size;

        if (*((*nfa).v as *mut vars)).spaceused >= REG_MAX_COMPILE_SPACE() {
            NERR(nfa, REG_ETOOBIG);
            return null_mut();
        }
        nstates = if !(*nfa).lastsb.is_null() {
            (*(*nfa).lastsb).nstates * 2
        } else {
            FIRSTSBSIZE as Size
        };
        if nstates > MAXSBSIZE as Size {
            nstates = MAXSBSIZE as Size;
        }
        newSb = MALLOC(STATEBATCHSIZE(nstates)) as *mut statebatch;
        if newSb.is_null() {
            NERR(nfa, REG_ESPACE);
            return null_mut();
        }
        (*((*nfa).v as *mut vars)).spaceused += STATEBATCHSIZE(nstates);
        (*newSb).nstates = nstates;
        (*newSb).next = (*nfa).lastsb;
        (*nfa).lastsb = newSb;
        (*nfa).lastsbused = 1;
        s = (*newSb).s.as_mut_ptr().add(0);
    }

    Assert!((*nfa).nstates >= 0);
    (*s).no = (*nfa).nstates;
    (*nfa).nstates += 1;
    (*s).flag = 0;
    if (*nfa).states.is_null() {
        (*nfa).states = s;
    }
    (*s).nins = 0;
    (*s).ins = null_mut();
    (*s).nouts = 0;
    (*s).outs = null_mut();
    (*s).tmp = null_mut();
    (*s).next = null_mut();
    if !(*nfa).slast.is_null() {
        Assert!((*(*nfa).slast).next.is_null());
        (*(*nfa).slast).next = s;
    }
    (*s).prev = (*nfa).slast;
    (*nfa).slast = s;
    s
}

/*
 * newfstate - allocate an NFA state with a specified flag value
 */
pub unsafe fn newfstate(nfa: *mut nfa, flag: c_int) -> *mut state /* NULL on error */ {
    let s: *mut state;

    s = newstate(nfa);
    if !s.is_null() {
        (*s).flag = flag as c_char;
    }
    s
}

/*
 * dropstate - delete a state's inarcs and outarcs and free it
 */
pub unsafe fn dropstate(nfa: *mut nfa, s: *mut state) {
    let mut a: *mut arc;

    loop {
        a = (*s).ins;
        if a.is_null() {
            break;
        }
        freearc(nfa, a);
    }
    loop {
        a = (*s).outs;
        if a.is_null() {
            break;
        }
        freearc(nfa, a);
    }
    freestate(nfa, s);
}

/*
 * freestate - free a state, which has no in-arcs or out-arcs
 */
pub unsafe fn freestate(nfa: *mut nfa, s: *mut state) {
    Assert!(!s.is_null());
    Assert!((*s).nins == 0 && (*s).nouts == 0);

    (*s).no = FREESTATE;
    (*s).flag = 0;
    if !(*s).next.is_null() {
        (*(*s).next).prev = (*s).prev;
    } else {
        Assert!(s == (*nfa).slast);
        (*nfa).slast = (*s).prev;
    }
    if !(*s).prev.is_null() {
        (*(*s).prev).next = (*s).next;
    } else {
        Assert!(s == (*nfa).states);
        (*nfa).states = (*s).next;
    }
    (*s).prev = null_mut();
    (*s).next = (*nfa).freestates; /* don't delete it, put it on the free list */
    (*nfa).freestates = s;
}

/*
 * newarc - set up a new arc within an NFA
 *
 * This function checks to make sure that no duplicate arcs are created.
 * In general we never want duplicates.
 *
 * However: in principle, a RAINBOW arc is redundant with any plain arc
 * (unless that arc is for a pseudocolor).  But we don't try to recognize
 * that redundancy, either here or in allied operations such as moveins().
 * The pseudocolor consideration makes that more costly than it seems worth.
 */
pub unsafe fn newarc(nfa: *mut nfa, t: c_int, co: color, from: *mut state, to: *mut state) {
    let mut a: *mut arc;

    Assert!(!from.is_null() && !to.is_null());

    /*
     * This is a handy place to check for operation cancel during regex
     * compilation, since no code path will go very long without making a new
     * state or arc.
     */
    INTERRUPT((*((*nfa).v as *mut vars)).re);

    /* check for duplicate arc, using whichever chain is shorter */
    if (*from).nouts <= (*to).nins {
        a = (*from).outs;
        while !a.is_null() {
            if (*a).to == to && (*a).co == co && (*a).r#type == t {
                return;
            }
            a = (*a).outchain;
        }
    } else {
        a = (*to).ins;
        while !a.is_null() {
            if (*a).from == from && (*a).co == co && (*a).r#type == t {
                return;
            }
            a = (*a).inchain;
        }
    }

    /* no dup, so create the arc */
    createarc(nfa, t, co, from, to);
}

/*
 * createarc - create a new arc within an NFA
 *
 * This function must *only* be used after verifying that there is no existing
 * identical arc (same type/color/from/to).
 */
pub unsafe fn createarc(nfa: *mut nfa, t: c_int, co: color, from: *mut state, to: *mut state) {
    let a: *mut arc;

    a = allocarc(nfa);
    if NISERR(nfa) {
        return;
    }
    Assert!(!a.is_null());

    (*a).r#type = t;
    (*a).co = co;
    (*a).to = to;
    (*a).from = from;

    /*
     * Put the new arc on the beginning, not the end, of the chains; it's
     * simpler here, and freearc() is the same cost either way.  See also the
     * logic in moveins() and its cohorts, as well as fixempties().
     */
    (*a).inchain = (*to).ins;
    (*a).inchainRev = null_mut();
    if !(*to).ins.is_null() {
        (*(*to).ins).inchainRev = a;
    }
    (*to).ins = a;
    (*a).outchain = (*from).outs;
    (*a).outchainRev = null_mut();
    if !(*from).outs.is_null() {
        (*(*from).outs).outchainRev = a;
    }
    (*from).outs = a;

    (*from).nouts += 1;
    (*to).nins += 1;

    if COLORED(a) && (*nfa).parent.is_null() {
        colorchain((*nfa).cm, a);
    }
}

/*
 * allocarc - allocate a new arc within an NFA
 */
pub unsafe fn allocarc(nfa: *mut nfa) -> *mut arc /* NULL for failure */ {
    let a: *mut arc;

    /* first, recycle anything that's on the freelist */
    if !(*nfa).freearcs.is_null() {
        a = (*nfa).freearcs;
        (*nfa).freearcs = (*a).outchain;
    }
    /* otherwise, is there anything left in the last arcbatch? */
    else if !(*nfa).lastab.is_null() && (*nfa).lastabused < (*(*nfa).lastab).narcs {
        a = (*(*nfa).lastab).a.as_mut_ptr().add((*nfa).lastabused);
        (*nfa).lastabused += 1;
    }
    /* otherwise, need to allocate a new arcbatch */
    else {
        let newAb: *mut arcbatch;
        let mut narcs: Size;

        if (*((*nfa).v as *mut vars)).spaceused >= REG_MAX_COMPILE_SPACE() {
            NERR(nfa, REG_ETOOBIG);
            return null_mut();
        }
        narcs = if !(*nfa).lastab.is_null() {
            (*(*nfa).lastab).narcs * 2
        } else {
            FIRSTABSIZE as Size
        };
        if narcs > MAXABSIZE as Size {
            narcs = MAXABSIZE as Size;
        }
        newAb = MALLOC(ARCBATCHSIZE(narcs)) as *mut arcbatch;
        if newAb.is_null() {
            NERR(nfa, REG_ESPACE);
            return null_mut();
        }
        (*((*nfa).v as *mut vars)).spaceused += ARCBATCHSIZE(narcs);
        (*newAb).narcs = narcs;
        (*newAb).next = (*nfa).lastab;
        (*nfa).lastab = newAb;
        (*nfa).lastabused = 1;
        a = (*newAb).a.as_mut_ptr().add(0);
    }

    a
}

/*
 * freearc - free an arc
 */
pub unsafe fn freearc(nfa: *mut nfa, victim: *mut arc) {
    let from: *mut state = (*victim).from;
    let to: *mut state = (*victim).to;
    let mut predecessor: *mut arc;

    Assert!((*victim).r#type != 0);

    /* take it off color chain if necessary */
    if COLORED(victim) && (*nfa).parent.is_null() {
        uncolorchain((*nfa).cm, victim);
    }

    /* take it off source's out-chain */
    Assert!(!from.is_null());
    predecessor = (*victim).outchainRev;
    if predecessor.is_null() {
        Assert!((*from).outs == victim);
        (*from).outs = (*victim).outchain;
    } else {
        Assert!((*predecessor).outchain == victim);
        (*predecessor).outchain = (*victim).outchain;
    }
    if !(*victim).outchain.is_null() {
        Assert!((*(*victim).outchain).outchainRev == victim);
        (*(*victim).outchain).outchainRev = predecessor;
    }
    (*from).nouts -= 1;

    /* take it off target's in-chain */
    Assert!(!to.is_null());
    predecessor = (*victim).inchainRev;
    if predecessor.is_null() {
        Assert!((*to).ins == victim);
        (*to).ins = (*victim).inchain;
    } else {
        Assert!((*predecessor).inchain == victim);
        (*predecessor).inchain = (*victim).inchain;
    }
    if !(*victim).inchain.is_null() {
        Assert!((*(*victim).inchain).inchainRev == victim);
        (*(*victim).inchain).inchainRev = predecessor;
    }
    (*to).nins -= 1;

    /* clean up and place on NFA's free list */
    (*victim).r#type = 0;
    (*victim).from = null_mut(); /* precautions... */
    (*victim).to = null_mut();
    (*victim).inchain = null_mut();
    (*victim).inchainRev = null_mut();
    (*victim).outchain = null_mut();
    (*victim).outchainRev = null_mut();
    (*victim).outchain = (*nfa).freearcs;
    (*nfa).freearcs = victim;
}

/*
 * changearcsource - flip an arc to have a different from state
 *
 * Caller must have verified that there is no pre-existing duplicate arc.
 */
pub unsafe fn changearcsource(a: *mut arc, newfrom: *mut state) {
    let oldfrom: *mut state = (*a).from;
    let predecessor: *mut arc;

    Assert!(oldfrom != newfrom);

    /* take it off old source's out-chain */
    Assert!(!oldfrom.is_null());
    predecessor = (*a).outchainRev;
    if predecessor.is_null() {
        Assert!((*oldfrom).outs == a);
        (*oldfrom).outs = (*a).outchain;
    } else {
        Assert!((*predecessor).outchain == a);
        (*predecessor).outchain = (*a).outchain;
    }
    if !(*a).outchain.is_null() {
        Assert!((*(*a).outchain).outchainRev == a);
        (*(*a).outchain).outchainRev = predecessor;
    }
    (*oldfrom).nouts -= 1;

    (*a).from = newfrom;

    /* prepend it to new source's out-chain */
    (*a).outchain = (*newfrom).outs;
    (*a).outchainRev = null_mut();
    if !(*newfrom).outs.is_null() {
        (*(*newfrom).outs).outchainRev = a;
    }
    (*newfrom).outs = a;
    (*newfrom).nouts += 1;
}

/*
 * changearctarget - flip an arc to have a different to state
 *
 * Caller must have verified that there is no pre-existing duplicate arc.
 */
pub unsafe fn changearctarget(a: *mut arc, newto: *mut state) {
    let oldto: *mut state = (*a).to;
    let predecessor: *mut arc;

    Assert!(oldto != newto);

    /* take it off old target's in-chain */
    Assert!(!oldto.is_null());
    predecessor = (*a).inchainRev;
    if predecessor.is_null() {
        Assert!((*oldto).ins == a);
        (*oldto).ins = (*a).inchain;
    } else {
        Assert!((*predecessor).inchain == a);
        (*predecessor).inchain = (*a).inchain;
    }
    if !(*a).inchain.is_null() {
        Assert!((*(*a).inchain).inchainRev == a);
        (*(*a).inchain).inchainRev = predecessor;
    }
    (*oldto).nins -= 1;

    (*a).to = newto;

    /* prepend it to new target's in-chain */
    (*a).inchain = (*newto).ins;
    (*a).inchainRev = null_mut();
    if !(*newto).ins.is_null() {
        (*(*newto).ins).inchainRev = a;
    }
    (*newto).ins = a;
    (*newto).nins += 1;
}

/*
 * hasnonemptyout - Does state have a non-EMPTY out arc?
 */
pub unsafe fn hasnonemptyout(s: *mut state) -> c_int {
    let mut a: *mut arc;

    a = (*s).outs;
    while !a.is_null() {
        if (*a).r#type != EMPTY {
            return 1;
        }
        a = (*a).outchain;
    }
    0
}

/*
 * findarc - find arc, if any, from given source with given type and color
 * If there is more than one such arc, the result is random.
 */
pub unsafe fn findarc(s: *mut state, r#type: c_int, co: color) -> *mut arc {
    let mut a: *mut arc;

    a = (*s).outs;
    while !a.is_null() {
        if (*a).r#type == r#type && (*a).co == co {
            return a;
        }
        a = (*a).outchain;
    }
    null_mut()
}

/*
 * cparc - allocate a new arc within an NFA, copying details from old one
 */
pub unsafe fn cparc(nfa: *mut nfa, oa: *mut arc, from: *mut state, to: *mut state) {
    newarc(nfa, (*oa).r#type, (*oa).co, from, to);
}

/*
 * sortins - sort the in arcs of a state by from/color/type
 */
pub unsafe fn sortins(nfa: *mut nfa, s: *mut state) {
    let sortarray: *mut *mut arc;
    let mut a: *mut arc;
    let n: c_int = (*s).nins;
    let mut i: c_int;

    if n <= 1 {
        return; /* nothing to do */
    }
    /* make an array of arc pointers ... */
    sortarray = MALLOC(n as usize * core::mem::size_of::<*mut arc>()) as *mut *mut arc;
    if sortarray.is_null() {
        NERR(nfa, REG_ESPACE);
        return;
    }
    i = 0;
    a = (*s).ins;
    while !a.is_null() {
        *sortarray.add(i as usize) = a;
        i += 1;
        a = (*a).inchain;
    }
    Assert!(i == n);
    /* ... sort the array */
    pg_qsort(
        sortarray as *mut c_void,
        n as usize,
        core::mem::size_of::<*mut arc>(),
        sortins_cmp,
    );
    /* ... and rebuild arc list in order */
    /* it seems worth special-casing first and last items to simplify loop */
    a = *sortarray.add(0);
    (*s).ins = a;
    (*a).inchain = *sortarray.add(1);
    (*a).inchainRev = null_mut();
    i = 1;
    while i < n - 1 {
        a = *sortarray.add(i as usize);
        (*a).inchain = *sortarray.add((i + 1) as usize);
        (*a).inchainRev = *sortarray.add((i - 1) as usize);
        i += 1;
    }
    a = *sortarray.add(i as usize);
    (*a).inchain = null_mut();
    (*a).inchainRev = *sortarray.add((i - 1) as usize);
    FREE(sortarray as *mut c_void);
}

unsafe fn sortins_cmp(a: *const c_void, b: *const c_void) -> c_int {
    let aa: *const arc = *(a as *const *const arc);
    let bb: *const arc = *(b as *const *const arc);

    /* we check the fields in the order they are most likely to be different */
    if (*(*aa).from).no < (*(*bb).from).no {
        return -1;
    }
    if (*(*aa).from).no > (*(*bb).from).no {
        return 1;
    }
    if (*aa).co < (*bb).co {
        return -1;
    }
    if (*aa).co > (*bb).co {
        return 1;
    }
    if (*aa).r#type < (*bb).r#type {
        return -1;
    }
    if (*aa).r#type > (*bb).r#type {
        return 1;
    }
    0
}

/*
 * sortouts - sort the out arcs of a state by to/color/type
 */
pub unsafe fn sortouts(nfa: *mut nfa, s: *mut state) {
    let sortarray: *mut *mut arc;
    let mut a: *mut arc;
    let n: c_int = (*s).nouts;
    let mut i: c_int;

    if n <= 1 {
        return; /* nothing to do */
    }
    /* make an array of arc pointers ... */
    sortarray = MALLOC(n as usize * core::mem::size_of::<*mut arc>()) as *mut *mut arc;
    if sortarray.is_null() {
        NERR(nfa, REG_ESPACE);
        return;
    }
    i = 0;
    a = (*s).outs;
    while !a.is_null() {
        *sortarray.add(i as usize) = a;
        i += 1;
        a = (*a).outchain;
    }
    Assert!(i == n);
    /* ... sort the array */
    pg_qsort(
        sortarray as *mut c_void,
        n as usize,
        core::mem::size_of::<*mut arc>(),
        sortouts_cmp,
    );
    /* ... and rebuild arc list in order */
    /* it seems worth special-casing first and last items to simplify loop */
    a = *sortarray.add(0);
    (*s).outs = a;
    (*a).outchain = *sortarray.add(1);
    (*a).outchainRev = null_mut();
    i = 1;
    while i < n - 1 {
        a = *sortarray.add(i as usize);
        (*a).outchain = *sortarray.add((i + 1) as usize);
        (*a).outchainRev = *sortarray.add((i - 1) as usize);
        i += 1;
    }
    a = *sortarray.add(i as usize);
    (*a).outchain = null_mut();
    (*a).outchainRev = *sortarray.add((i - 1) as usize);
    FREE(sortarray as *mut c_void);
}

unsafe fn sortouts_cmp(a: *const c_void, b: *const c_void) -> c_int {
    let aa: *const arc = *(a as *const *const arc);
    let bb: *const arc = *(b as *const *const arc);

    /* we check the fields in the order they are most likely to be different */
    if (*(*aa).to).no < (*(*bb).to).no {
        return -1;
    }
    if (*(*aa).to).no > (*(*bb).to).no {
        return 1;
    }
    if (*aa).co < (*bb).co {
        return -1;
    }
    if (*aa).co > (*bb).co {
        return 1;
    }
    if (*aa).r#type < (*bb).r#type {
        return -1;
    }
    if (*aa).r#type > (*bb).r#type {
        return 1;
    }
    0
}

/*
 * Common decision logic about whether to use arc-by-arc operations or
 * sort/merge.  If there's just a few source arcs we cannot recoup the
 * cost of sorting the destination arc list, no matter how large it is.
 * Otherwise, limit the number of arc-by-arc comparisons to about 1000
 * (a somewhat arbitrary choice, but the breakeven point would probably
 * be machine dependent anyway).
 *
 * #define BULK_ARC_OP_USE_SORT(nsrcarcs, ndestarcs) \
 *   ((nsrcarcs) < 4 ? 0 : ((nsrcarcs) > 32 || (ndestarcs) > 32))
 */
#[inline]
fn BULK_ARC_OP_USE_SORT(nsrcarcs: c_int, ndestarcs: c_int) -> bool {
    if nsrcarcs < 4 {
        false
    } else {
        nsrcarcs > 32 || ndestarcs > 32
    }
}

/*
 * moveins - move all in arcs of a state to another state
 *
 * You might think this could be done better by just updating the
 * existing arcs, and you would be right if it weren't for the need
 * for duplicate suppression, which makes it easier to just make new
 * ones to exploit the suppression built into newarc.
 *
 * However, if we have a whole lot of arcs to deal with, retail duplicate
 * checks become too slow.  In that case we proceed by sorting and merging
 * the arc lists, and then we can indeed just update the arcs in-place.
 *
 * On the other hand, it's also true that this is frequently called with
 * a brand-new newState that has no existing in-arcs.  In that case,
 * de-duplication is unnecessary, so we can just blindly move all the arcs.
 */
pub unsafe fn moveins(nfa: *mut nfa, oldState: *mut state, newState: *mut state) {
    Assert!(oldState != newState);

    if (*newState).nins == 0 {
        /* No need for de-duplication */
        let mut a: *mut arc;

        loop {
            a = (*oldState).ins;
            if a.is_null() {
                break;
            }
            createarc(nfa, (*a).r#type, (*a).co, (*a).from, newState);
            freearc(nfa, a);
        }
    } else if !BULK_ARC_OP_USE_SORT((*oldState).nins, (*newState).nins) {
        /* With not too many arcs, just do them one at a time */
        let mut a: *mut arc;

        loop {
            a = (*oldState).ins;
            if a.is_null() {
                break;
            }
            cparc(nfa, a, (*a).from, newState);
            freearc(nfa, a);
        }
    } else {
        /*
         * With many arcs, use a sort-merge approach.  Note changearctarget()
         * will put the arc onto the front of newState's chain, so it does not
         * break our walk through the sorted part of the chain.
         */
        let mut oa: *mut arc;
        let mut na: *mut arc;

        /*
         * Because we bypass newarc() in this code path, we'd better include a
         * cancel check.
         */
        INTERRUPT((*((*nfa).v as *mut vars)).re);

        sortins(nfa, oldState);
        sortins(nfa, newState);
        if NISERR(nfa) {
            return; /* might have failed to sort */
        }
        oa = (*oldState).ins;
        na = (*newState).ins;
        while !oa.is_null() && !na.is_null() {
            let a: *mut arc = oa;

            match sortins_cmp(
                &oa as *const *mut arc as *const c_void,
                &na as *const *mut arc as *const c_void,
            ) {
                -1 => {
                    /* newState does not have anything matching oa */
                    oa = (*oa).inchain;

                    /*
                     * Rather than doing createarc+freearc, we can just unlink
                     * and relink the existing arc struct.
                     */
                    changearctarget(a, newState);
                }
                0 => {
                    /* match, advance in both lists */
                    oa = (*oa).inchain;
                    na = (*na).inchain;
                    /* ... and drop duplicate arc from oldState */
                    freearc(nfa, a);
                }
                1 => {
                    /* advance only na; oa might have a match later */
                    na = (*na).inchain;
                }
                _ => {
                    Assert!(NOTREACHED != 0);
                }
            }
        }
        while !oa.is_null() {
            /* newState does not have anything matching oa */
            let a: *mut arc = oa;

            oa = (*oa).inchain;
            changearctarget(a, newState);
        }
    }

    Assert!((*oldState).nins == 0);
    Assert!((*oldState).ins.is_null());
}

/*
 * copyins - copy in arcs of a state to another state
 *
 * The comments for moveins() apply here as well.  However, in current
 * usage, this is *only* called with brand-new target states, so that
 * only the "no need for de-duplication" code path is ever reached.
 * We keep the rest #ifdef'd out in case it's needed in the future.
 */
pub unsafe fn copyins(nfa: *mut nfa, oldState: *mut state, newState: *mut state) {
    let _ = nfa;
    Assert!(oldState != newState);
    Assert!((*newState).nins == 0); /* see comment above */

    if (*newState).nins == 0 {
        /* No need for de-duplication */
        let mut a: *mut arc;

        a = (*oldState).ins;
        while !a.is_null() {
            createarc(nfa, (*a).r#type, (*a).co, (*a).from, newState);
            a = (*a).inchain;
        }
    }
    // #ifdef NOT_USED -- alternative bulk path retained but compiled out in C.
}

/*
 * mergeins - merge a list of inarcs into a state
 *
 * This is much like copyins, but the source arcs are listed in an array,
 * and are not guaranteed unique.  It's okay to clobber the array contents.
 */
pub unsafe fn mergeins(nfa: *mut nfa, s: *mut state, arcarray: *mut *mut arc, arccount: c_int) {
    let mut na: *mut arc;
    let mut i: c_int;
    let mut j: c_int;
    let mut arccount = arccount;

    if arccount <= 0 {
        return;
    }

    /*
     * Because we bypass newarc() in this code path, we'd better include a
     * cancel check.
     */
    INTERRUPT((*((*nfa).v as *mut vars)).re);

    /* Sort existing inarcs as well as proposed new ones */
    sortins(nfa, s);
    if NISERR(nfa) {
        return; /* might have failed to sort */
    }

    pg_qsort(
        arcarray as *mut c_void,
        arccount as usize,
        core::mem::size_of::<*mut arc>(),
        sortins_cmp,
    );

    /*
     * arcarray very likely includes dups, so we must eliminate them.  (This
     * could be folded into the next loop, but it's not worth the trouble.)
     */
    j = 0;
    i = 1;
    while i < arccount {
        match sortins_cmp(
            arcarray.add(j as usize) as *const c_void,
            arcarray.add(i as usize) as *const c_void,
        ) {
            -1 => {
                /* non-dup */
                j += 1;
                *arcarray.add(j as usize) = *arcarray.add(i as usize);
            }
            0 => {
                /* dup */
            }
            _ => {
                /* trouble */
                Assert!(NOTREACHED != 0);
            }
        }
        i += 1;
    }
    arccount = j + 1;

    /*
     * Now merge into s' inchain.  Note that createarc() will put new arcs
     * onto the front of s's chain, so it does not break our walk through the
     * sorted part of the chain.
     */
    i = 0;
    na = (*s).ins;
    while i < arccount && !na.is_null() {
        let a: *mut arc = *arcarray.add(i as usize);

        match sortins_cmp(
            &a as *const *mut arc as *const c_void,
            &na as *const *mut arc as *const c_void,
        ) {
            -1 => {
                /* s does not have anything matching a */
                createarc(nfa, (*a).r#type, (*a).co, (*a).from, s);
                i += 1;
            }
            0 => {
                /* match, advance in both lists */
                i += 1;
                na = (*na).inchain;
            }
            1 => {
                /* advance only na; array might have a match later */
                na = (*na).inchain;
            }
            _ => {
                Assert!(NOTREACHED != 0);
            }
        }
    }
    while i < arccount {
        /* s does not have anything matching a */
        let a: *mut arc = *arcarray.add(i as usize);

        createarc(nfa, (*a).r#type, (*a).co, (*a).from, s);
        i += 1;
    }
}

/*
 * moveouts - move all out arcs of a state to another state
 *
 * See comments for moveins()
 */
pub unsafe fn moveouts(nfa: *mut nfa, oldState: *mut state, newState: *mut state) {
    Assert!(oldState != newState);

    if (*newState).nouts == 0 {
        /* No need for de-duplication */
        let mut a: *mut arc;

        loop {
            a = (*oldState).outs;
            if a.is_null() {
                break;
            }
            createarc(nfa, (*a).r#type, (*a).co, newState, (*a).to);
            freearc(nfa, a);
        }
    } else if !BULK_ARC_OP_USE_SORT((*oldState).nouts, (*newState).nouts) {
        /* With not too many arcs, just do them one at a time */
        let mut a: *mut arc;

        loop {
            a = (*oldState).outs;
            if a.is_null() {
                break;
            }
            cparc(nfa, a, newState, (*a).to);
            freearc(nfa, a);
        }
    } else {
        /*
         * With many arcs, use a sort-merge approach.  Note changearcsource()
         * will put the arc onto the front of newState's chain, so it does not
         * break our walk through the sorted part of the chain.
         */
        let mut oa: *mut arc;
        let mut na: *mut arc;

        /*
         * Because we bypass newarc() in this code path, we'd better include a
         * cancel check.
         */
        INTERRUPT((*((*nfa).v as *mut vars)).re);

        sortouts(nfa, oldState);
        sortouts(nfa, newState);
        if NISERR(nfa) {
            return; /* might have failed to sort */
        }
        oa = (*oldState).outs;
        na = (*newState).outs;
        while !oa.is_null() && !na.is_null() {
            let a: *mut arc = oa;

            match sortouts_cmp(
                &oa as *const *mut arc as *const c_void,
                &na as *const *mut arc as *const c_void,
            ) {
                -1 => {
                    /* newState does not have anything matching oa */
                    oa = (*oa).outchain;

                    /*
                     * Rather than doing createarc+freearc, we can just unlink
                     * and relink the existing arc struct.
                     */
                    changearcsource(a, newState);
                }
                0 => {
                    /* match, advance in both lists */
                    oa = (*oa).outchain;
                    na = (*na).outchain;
                    /* ... and drop duplicate arc from oldState */
                    freearc(nfa, a);
                }
                1 => {
                    /* advance only na; oa might have a match later */
                    na = (*na).outchain;
                }
                _ => {
                    Assert!(NOTREACHED != 0);
                }
            }
        }
        while !oa.is_null() {
            /* newState does not have anything matching oa */
            let a: *mut arc = oa;

            oa = (*oa).outchain;
            changearcsource(a, newState);
        }
    }

    Assert!((*oldState).nouts == 0);
    Assert!((*oldState).outs.is_null());
}

/*
 * copyouts - copy out arcs of a state to another state
 *
 * See comments for copyins()
 */
pub unsafe fn copyouts(nfa: *mut nfa, oldState: *mut state, newState: *mut state) {
    let _ = nfa;
    Assert!(oldState != newState);
    Assert!((*newState).nouts == 0); /* see comment above */

    if (*newState).nouts == 0 {
        /* No need for de-duplication */
        let mut a: *mut arc;

        a = (*oldState).outs;
        while !a.is_null() {
            createarc(nfa, (*a).r#type, (*a).co, newState, (*a).to);
            a = (*a).outchain;
        }
    }
    // #ifdef NOT_USED -- alternative bulk path retained but compiled out in C.
}

/*
 * cloneouts - copy out arcs of a state to another state pair, modifying type
 *
 * This is only used to convert PLAIN arcs to AHEAD/BEHIND arcs, which share
 * the same interpretation of "co".  It wouldn't be sensible with LACONs.
 */
pub unsafe fn cloneouts(
    nfa: *mut nfa,
    old: *mut state,
    from: *mut state,
    to: *mut state,
    r#type: c_int,
) {
    let mut a: *mut arc;

    Assert!(old != from);
    Assert!(r#type == AHEAD || r#type == BEHIND);

    a = (*old).outs;
    while !a.is_null() {
        Assert!((*a).r#type == PLAIN);
        newarc(nfa, r#type, (*a).co, from, to);
        a = (*a).outchain;
    }
}

/*
 * delsub - delete a sub-NFA, updating subre pointers if necessary
 *
 * This uses a recursive traversal of the sub-NFA, marking already-seen
 * states using their tmp pointer.
 */
pub unsafe fn delsub(
    nfa: *mut nfa,
    lp: *mut state, /* the sub-NFA goes from here... */
    rp: *mut state, /* ...to here, *not* inclusive */
) {
    Assert!(lp != rp);

    (*rp).tmp = rp; /* mark end */

    deltraverse(nfa, lp, lp);
    if NISERR(nfa) {
        return; /* asserts might not hold after failure */
    }
    Assert!((*lp).nouts == 0 && (*rp).nins == 0); /* did the job */
    Assert!((*lp).no != FREESTATE && (*rp).no != FREESTATE); /* no more */

    (*rp).tmp = null_mut(); /* unmark end */
    (*lp).tmp = null_mut(); /* and begin, marked by deltraverse */
}

/*
 * deltraverse - the recursive heart of delsub
 * This routine's basic job is to destroy all out-arcs of the state.
 */
pub unsafe fn deltraverse(nfa: *mut nfa, leftend: *mut state, s: *mut state) {
    let mut a: *mut arc;
    let mut to: *mut state;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return;
    }

    if (*s).nouts == 0 {
        return; /* nothing to do */
    }
    if !(*s).tmp.is_null() {
        return; /* already in progress */
    }

    (*s).tmp = s; /* mark as in progress */

    loop {
        a = (*s).outs;
        if a.is_null() {
            break;
        }
        to = (*a).to;
        deltraverse(nfa, leftend, to);
        if NISERR(nfa) {
            return; /* asserts might not hold after failure */
        }
        Assert!((*to).nouts == 0 || !(*to).tmp.is_null());
        freearc(nfa, a);
        if (*to).nins == 0 && (*to).tmp.is_null() {
            Assert!((*to).nouts == 0);
            freestate(nfa, to);
        }
    }

    Assert!((*s).no != FREESTATE); /* we're still here */
    Assert!(s == leftend || (*s).nins != 0); /* and still reachable */
    Assert!((*s).nouts == 0); /* but have no outarcs */

    (*s).tmp = null_mut(); /* we're done here */
}

/*
 * dupnfa - duplicate sub-NFA
 *
 * Another recursive traversal, this time using tmp to point to duplicates
 * as well as mark already-seen states.  (You knew there was a reason why
 * it's a state pointer, didn't you? :-))
 */
pub unsafe fn dupnfa(
    nfa: *mut nfa,
    start: *mut state, /* duplicate of subNFA starting here */
    stop: *mut state,  /* and stopping here */
    from: *mut state,  /* stringing duplicate from here */
    to: *mut state,    /* to here */
) {
    if start == stop {
        newarc(nfa, EMPTY, 0, from, to);
        return;
    }

    (*stop).tmp = to;
    duptraverse(nfa, start, from);
    /* done, except for clearing out the tmp pointers */

    (*stop).tmp = null_mut();
    cleartraverse(nfa, start);
}

/*
 * duptraverse - recursive heart of dupnfa
 */
pub unsafe fn duptraverse(
    nfa: *mut nfa,
    s: *mut state,
    stmp: *mut state, /* s's duplicate, or NULL */
) {
    let mut a: *mut arc;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return;
    }

    if !(*s).tmp.is_null() {
        return; /* already done */
    }

    (*s).tmp = if stmp.is_null() { newstate(nfa) } else { stmp };
    if (*s).tmp.is_null() {
        Assert!(NISERR(nfa));
        return;
    }

    a = (*s).outs;
    while !a.is_null() && !NISERR(nfa) {
        duptraverse(nfa, (*a).to, null_mut());
        if NISERR(nfa) {
            break;
        }
        Assert!(!(*(*a).to).tmp.is_null());
        cparc(nfa, a, (*s).tmp, (*(*a).to).tmp);
        a = (*a).outchain;
    }
}

/*
 * removeconstraints - remove any constraints in an NFA
 *
 * Constraint arcs are replaced by empty arcs, essentially treating all
 * constraints as automatically satisfied.
 */
pub unsafe fn removeconstraints(
    nfa: *mut nfa,
    start: *mut state, /* process subNFA starting here */
    stop: *mut state,  /* and stopping here */
) {
    if start == stop {
        return;
    }

    (*stop).tmp = stop;
    removetraverse(nfa, start);
    /* done, except for clearing out the tmp pointers */

    (*stop).tmp = null_mut();
    cleartraverse(nfa, start);
}

/*
 * removetraverse - recursive heart of removeconstraints
 */
pub unsafe fn removetraverse(nfa: *mut nfa, s: *mut state) {
    let mut a: *mut arc;
    let mut oa: *mut arc;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return;
    }

    if !(*s).tmp.is_null() {
        return; /* already done */
    }

    (*s).tmp = s;
    a = (*s).outs;
    while !a.is_null() && !NISERR(nfa) {
        removetraverse(nfa, (*a).to);
        if NISERR(nfa) {
            break;
        }
        oa = (*a).outchain;
        match (*a).r#type {
            t if t == PLAIN || t == EMPTY || t == CANTMATCH => {
                /* nothing to do */
            }
            t if t == AHEAD
                || t == BEHIND
                || t == b'^' as c_int
                || t == b'$' as c_int
                || t == LACON =>
            {
                /* replace it */
                newarc(nfa, EMPTY, 0, s, (*a).to);
                freearc(nfa, a);
            }
            _ => {
                NERR(nfa, REG_ASSERT);
            }
        }
        a = oa;
    }
}

/*
 * cleartraverse - recursive cleanup for algorithms that leave tmp ptrs set
 */
pub unsafe fn cleartraverse(nfa: *mut nfa, s: *mut state) {
    let mut a: *mut arc;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return;
    }

    if (*s).tmp.is_null() {
        return;
    }
    (*s).tmp = null_mut();

    a = (*s).outs;
    while !a.is_null() {
        cleartraverse(nfa, (*a).to);
        a = (*a).outchain;
    }
}

/*
 * single_color_transition - does getting from s1 to s2 cross one PLAIN arc?
 *
 * If traversing from s1 to s2 requires a single PLAIN match (possibly of any
 * of a set of colors), return a state whose outarc list contains only PLAIN
 * arcs of those color(s).  Otherwise return NULL.
 *
 * This is used before optimizing the NFA, so there may be EMPTY arcs, which
 * we should ignore; the possibility of an EMPTY is why the result state could
 * be different from s1.
 *
 * It's worth troubling to handle multiple parallel PLAIN arcs here because a
 * bracket construct such as [abc] might yield either one or several parallel
 * PLAIN arcs depending on earlier atoms in the expression.  We'd rather that
 * that implementation detail not create user-visible performance differences.
 */
pub unsafe fn single_color_transition(s1: *mut state, s2: *mut state) -> *mut state {
    let mut a: *mut arc;
    let mut s1 = s1;
    let mut s2 = s2;

    /* Ignore leading EMPTY arc, if any */
    if (*s1).nouts == 1 && (*(*s1).outs).r#type == EMPTY {
        s1 = (*(*s1).outs).to;
    }
    /* Likewise for any trailing EMPTY arc */
    if (*s2).nins == 1 && (*(*s2).ins).r#type == EMPTY {
        s2 = (*(*s2).ins).from;
    }
    /* Perhaps we could have a single-state loop in between, if so reject */
    if s1 == s2 {
        return null_mut();
    }
    /* s1 must have at least one outarc... */
    if (*s1).outs.is_null() {
        return null_mut();
    }
    /* ... and they must all be PLAIN arcs to s2 */
    a = (*s1).outs;
    while !a.is_null() {
        if (*a).r#type != PLAIN || (*a).to != s2 {
            return null_mut();
        }
        a = (*a).outchain;
    }
    /* OK, return s1 as the possessor of the relevant outarcs */
    s1
}

/*
 * specialcolors - fill in special colors for an NFA
 */
pub unsafe fn specialcolors(nfa: *mut nfa) {
    /* false colors for BOS, BOL, EOS, EOL */
    if (*nfa).parent.is_null() {
        (*nfa).bos[0] = pseudocolor((*nfa).cm);
        (*nfa).bos[1] = pseudocolor((*nfa).cm);
        (*nfa).eos[0] = pseudocolor((*nfa).cm);
        (*nfa).eos[1] = pseudocolor((*nfa).cm);
    } else {
        Assert!((*(*nfa).parent).bos[0] != COLORLESS);
        (*nfa).bos[0] = (*(*nfa).parent).bos[0];
        Assert!((*(*nfa).parent).bos[1] != COLORLESS);
        (*nfa).bos[1] = (*(*nfa).parent).bos[1];
        Assert!((*(*nfa).parent).eos[0] != COLORLESS);
        (*nfa).eos[0] = (*(*nfa).parent).eos[0];
        Assert!((*(*nfa).parent).eos[1] != COLORLESS);
        (*nfa).eos[1] = (*(*nfa).parent).eos[1];
    }
}

/*
 * optimize - optimize an NFA
 *
 * The main goal of this function is not so much "optimization" (though it
 * does try to get rid of useless NFA states) as reducing the NFA to a form
 * the regex executor can handle.  The executor, and indeed the cNFA format
 * that is its input, can only handle PLAIN and LACON arcs.  The output of
 * the regex parser also includes EMPTY (do-nothing) arcs, as well as
 * ^, $, AHEAD, and BEHIND constraint arcs, which we must get rid of here.
 * We first get rid of EMPTY arcs and then deal with the constraint arcs.
 * The hardest part of either job is to get rid of circular loops of the
 * target arc type.  We would have to do that in any case, though, as such a
 * loop would otherwise allow the executor to cycle through the loop endlessly
 * without making any progress in the input string.
 */
pub unsafe fn optimize(
    nfa: *mut nfa,
    f: *mut c_void, /* FILE *; for debug output; NULL none */
) -> c_long /* re_info bits */ {
    // #ifdef REG_DEBUG -- verbose tracing omitted (REG_DEBUG undefined).

    /* If we have any CANTMATCH arcs, drop them; but this is uncommon */
    if (*nfa).flags & HASCANTMATCH != 0 {
        removecantmatch(nfa);
        (*nfa).flags &= !HASCANTMATCH;
    }
    cleanup(nfa); /* may simplify situation */
    fixempties(nfa, f); /* get rid of EMPTY arcs */
    fixconstraintloops(nfa, f); /* get rid of constraint loops */
    pullback(nfa, f); /* pull back constraints backward */
    pushfwd(nfa, f); /* push fwd constraints forward */
    cleanup(nfa); /* final tidying */
    analyze(nfa) /* and analysis */
}

/*
 * pullback - pull back constraints backward to eliminate them
 */
pub unsafe fn pullback(
    nfa: *mut nfa,
    f: *mut c_void, /* FILE *; for debug output; NULL none */
) {
    let mut s: *mut state;
    let mut nexts: *mut state;
    let mut a: *mut arc;
    let mut nexta: *mut arc;
    let mut intermediates: *mut state;
    let mut progress: c_int;

    /* find and pull until there are no more */
    loop {
        progress = 0;
        s = (*nfa).states;
        while !s.is_null() && !NISERR(nfa) {
            nexts = (*s).next;
            intermediates = null_mut();
            a = (*s).outs;
            while !a.is_null() && !NISERR(nfa) {
                nexta = (*a).outchain;
                if (*a).r#type == b'^' as c_int || (*a).r#type == BEHIND {
                    if pull(nfa, a, &mut intermediates) != 0 {
                        progress = 1;
                    }
                }
                a = nexta;
            }
            /* clear tmp fields of intermediate states created here */
            while !intermediates.is_null() {
                let ns: *mut state = (*intermediates).tmp;

                (*intermediates).tmp = null_mut();
                intermediates = ns;
            }
            /* if s is now useless, get rid of it */
            if ((*s).nins == 0 || (*s).nouts == 0) && (*s).flag == 0 {
                dropstate(nfa, s);
            }
            s = nexts;
        }
        if progress != 0 && !f.is_null() {
            dumpnfa(nfa, f);
        }
        if !(progress != 0 && !NISERR(nfa)) {
            break;
        }
    }
    if NISERR(nfa) {
        return;
    }

    /*
     * Any ^ constraints we were able to pull to the start state can now be
     * replaced by PLAIN arcs referencing the BOS or BOL colors.  There should
     * be no other ^ or BEHIND arcs left in the NFA, though we do not check
     * that here (compact() will fail if so).
     */
    a = (*(*nfa).pre).outs;
    while !a.is_null() {
        nexta = (*a).outchain;
        if (*a).r#type == b'^' as c_int {
            Assert!((*a).co == 0 || (*a).co == 1);
            newarc(nfa, PLAIN, (*nfa).bos[(*a).co as usize], (*a).from, (*a).to);
            freearc(nfa, a);
        }
        a = nexta;
    }
}

/*
 * pull - pull a back constraint backward past its source state
 *
 * Returns 1 if successful (which it always is unless the source is the
 * start state or we have an internal error), 0 if nothing happened.
 *
 * A significant property of this function is that it deletes no pre-existing
 * states, and no outarcs of the constraint's from state other than the given
 * constraint arc.  This makes the loops in pullback() safe, at the cost that
 * we may leave useless states behind.  Therefore, we leave it to pullback()
 * to delete such states.
 *
 * If the from state has multiple back-constraint outarcs, and/or multiple
 * compatible constraint inarcs, we only need to create one new intermediate
 * state per combination of predecessor and successor states.  *intermediates
 * points to a list of such intermediate states for this from state (chained
 * through their tmp fields).
 */
pub unsafe fn pull(nfa: *mut nfa, con: *mut arc, intermediates: *mut *mut state) -> c_int {
    let mut from: *mut state = (*con).from;
    let to: *mut state = (*con).to;
    let mut a: *mut arc;
    let mut nexta: *mut arc;
    let mut s: *mut state;
    let mut con = con;

    Assert!(from != to); /* should have gotten rid of this earlier */
    if (*from).flag != 0 {
        /* can't pull back beyond start */
        return 0;
    }
    if (*from).nins == 0 {
        /* unreachable */
        freearc(nfa, con);
        return 1;
    }

    /*
     * First, clone from state if necessary to avoid other outarcs.  This may
     * seem wasteful, but it simplifies the logic, and we'll get rid of the
     * clone state again at the bottom.
     */
    if (*from).nouts > 1 {
        s = newstate(nfa);
        if NISERR(nfa) {
            return 0;
        }
        copyins(nfa, from, s); /* duplicate inarcs */
        cparc(nfa, con, s, to); /* move constraint arc */
        freearc(nfa, con);
        if NISERR(nfa) {
            return 0;
        }
        from = s;
        con = (*from).outs;
    }
    Assert!((*from).nouts == 1);

    /* propagate the constraint into the from state's inarcs */
    a = (*from).ins;
    while !a.is_null() && !NISERR(nfa) {
        nexta = (*a).inchain;
        match combine(nfa, con, a) {
            c if c == INCOMPATIBLE => {
                /* destroy the arc */
                freearc(nfa, a);
            }
            c if c == SATISFIED => { /* no action needed */ }
            c if c == COMPATIBLE => {
                /* swap the two arcs, more or less */
                /* need an intermediate state, but might have one already */
                s = *intermediates;
                while !s.is_null() {
                    Assert!((*s).nins > 0 && (*s).nouts > 0);
                    if (*(*s).ins).from == (*a).from && (*(*s).outs).to == to {
                        break;
                    }
                    s = (*s).tmp;
                }
                if s.is_null() {
                    s = newstate(nfa);
                    if NISERR(nfa) {
                        return 0;
                    }
                    (*s).tmp = *intermediates;
                    *intermediates = s;
                }
                cparc(nfa, con, (*a).from, s);
                cparc(nfa, a, s, to);
                freearc(nfa, a);
            }
            c if c == REPLACEARC => {
                /* replace arc's color */
                newarc(nfa, (*a).r#type, (*con).co, (*a).from, to);
                freearc(nfa, a);
            }
            _ => {
                Assert!(NOTREACHED != 0);
            }
        }
        a = nexta;
    }

    /* remaining inarcs, if any, incorporate the constraint */
    moveins(nfa, from, to);
    freearc(nfa, con);
    /* from state is now useless, but we leave it to pullback() to clean up */
    1
}

/*
 * pushfwd - push forward constraints forward to eliminate them
 */
pub unsafe fn pushfwd(
    nfa: *mut nfa,
    f: *mut c_void, /* FILE *; for debug output; NULL none */
) {
    let mut s: *mut state;
    let mut nexts: *mut state;
    let mut a: *mut arc;
    let mut nexta: *mut arc;
    let mut intermediates: *mut state;
    let mut progress: c_int;

    /* find and push until there are no more */
    loop {
        progress = 0;
        s = (*nfa).states;
        while !s.is_null() && !NISERR(nfa) {
            nexts = (*s).next;
            intermediates = null_mut();
            a = (*s).ins;
            while !a.is_null() && !NISERR(nfa) {
                nexta = (*a).inchain;
                if (*a).r#type == b'$' as c_int || (*a).r#type == AHEAD {
                    if push(nfa, a, &mut intermediates) != 0 {
                        progress = 1;
                    }
                }
                a = nexta;
            }
            /* clear tmp fields of intermediate states created here */
            while !intermediates.is_null() {
                let ns: *mut state = (*intermediates).tmp;

                (*intermediates).tmp = null_mut();
                intermediates = ns;
            }
            /* if s is now useless, get rid of it */
            if ((*s).nins == 0 || (*s).nouts == 0) && (*s).flag == 0 {
                dropstate(nfa, s);
            }
            s = nexts;
        }
        if progress != 0 && !f.is_null() {
            dumpnfa(nfa, f);
        }
        if !(progress != 0 && !NISERR(nfa)) {
            break;
        }
    }
    if NISERR(nfa) {
        return;
    }

    /*
     * Any $ constraints we were able to push to the post state can now be
     * replaced by PLAIN arcs referencing the EOS or EOL colors.  There should
     * be no other $ or AHEAD arcs left in the NFA, though we do not check
     * that here (compact() will fail if so).
     */
    a = (*(*nfa).post).ins;
    while !a.is_null() {
        nexta = (*a).inchain;
        if (*a).r#type == b'$' as c_int {
            Assert!((*a).co == 0 || (*a).co == 1);
            newarc(nfa, PLAIN, (*nfa).eos[(*a).co as usize], (*a).from, (*a).to);
            freearc(nfa, a);
        }
        a = nexta;
    }
}

/*
 * push - push a forward constraint forward past its destination state
 *
 * Returns 1 if successful (which it always is unless the destination is the
 * post state or we have an internal error), 0 if nothing happened.
 *
 * A significant property of this function is that it deletes no pre-existing
 * states, and no inarcs of the constraint's to state other than the given
 * constraint arc.  This makes the loops in pushfwd() safe, at the cost that
 * we may leave useless states behind.  Therefore, we leave it to pushfwd()
 * to delete such states.
 *
 * If the to state has multiple forward-constraint inarcs, and/or multiple
 * compatible constraint outarcs, we only need to create one new intermediate
 * state per combination of predecessor and successor states.  *intermediates
 * points to a list of such intermediate states for this to state (chained
 * through their tmp fields).
 */
pub unsafe fn push(nfa: *mut nfa, con: *mut arc, intermediates: *mut *mut state) -> c_int {
    let from: *mut state = (*con).from;
    let mut to: *mut state = (*con).to;
    let mut a: *mut arc;
    let mut nexta: *mut arc;
    let mut s: *mut state;
    let mut con = con;

    Assert!(to != from); /* should have gotten rid of this earlier */
    if (*to).flag != 0 {
        /* can't push forward beyond end */
        return 0;
    }
    if (*to).nouts == 0 {
        /* dead end */
        freearc(nfa, con);
        return 1;
    }

    /*
     * First, clone to state if necessary to avoid other inarcs.  This may
     * seem wasteful, but it simplifies the logic, and we'll get rid of the
     * clone state again at the bottom.
     */
    if (*to).nins > 1 {
        s = newstate(nfa);
        if NISERR(nfa) {
            return 0;
        }
        copyouts(nfa, to, s); /* duplicate outarcs */
        cparc(nfa, con, from, s); /* move constraint arc */
        freearc(nfa, con);
        if NISERR(nfa) {
            return 0;
        }
        to = s;
        con = (*to).ins;
    }
    Assert!((*to).nins == 1);

    /* propagate the constraint into the to state's outarcs */
    a = (*to).outs;
    while !a.is_null() && !NISERR(nfa) {
        nexta = (*a).outchain;
        match combine(nfa, con, a) {
            c if c == INCOMPATIBLE => {
                /* destroy the arc */
                freearc(nfa, a);
            }
            c if c == SATISFIED => { /* no action needed */ }
            c if c == COMPATIBLE => {
                /* swap the two arcs, more or less */
                /* need an intermediate state, but might have one already */
                s = *intermediates;
                while !s.is_null() {
                    Assert!((*s).nins > 0 && (*s).nouts > 0);
                    if (*(*s).ins).from == from && (*(*s).outs).to == (*a).to {
                        break;
                    }
                    s = (*s).tmp;
                }
                if s.is_null() {
                    s = newstate(nfa);
                    if NISERR(nfa) {
                        return 0;
                    }
                    (*s).tmp = *intermediates;
                    *intermediates = s;
                }
                cparc(nfa, con, s, (*a).to);
                cparc(nfa, a, from, s);
                freearc(nfa, a);
            }
            c if c == REPLACEARC => {
                /* replace arc's color */
                newarc(nfa, (*a).r#type, (*con).co, from, (*a).to);
                freearc(nfa, a);
            }
            _ => {
                Assert!(NOTREACHED != 0);
            }
        }
        a = nexta;
    }

    /* remaining outarcs, if any, incorporate the constraint */
    moveouts(nfa, to, from);
    freearc(nfa, con);
    /* to state is now useless, but we leave it to pushfwd() to clean up */
    1
}

/*
 * combine - constraint lands on an arc, what happens?
 *
 * #def INCOMPATIBLE	1	// destroys arc
 * #def SATISFIED		2	// constraint satisfied
 * #def COMPATIBLE		3	// compatible but not satisfied yet
 * #def REPLACEARC		4	// replace arc's color with constraint color
 */
pub unsafe fn combine(nfa: *mut nfa, con: *mut arc, a: *mut arc) -> c_int {
    // #define CA(ct,at) (((ct)<<CHAR_BIT) | (at))
    #[inline]
    fn CA(ct: c_int, at: c_int) -> c_int {
        (ct << CHAR_BIT) | at
    }

    let key = CA((*con).r#type, (*a).r#type);

    if key == CA(b'^' as c_int, PLAIN) || key == CA(b'$' as c_int, PLAIN) {
        /* newlines are handled separately */
        return INCOMPATIBLE;
    }
    if key == CA(AHEAD, PLAIN) || key == CA(BEHIND, PLAIN) {
        /* color constraints meet colors */
        if (*con).co == (*a).co {
            return SATISFIED;
        }
        if (*con).co == RAINBOW {
            /* con is satisfied unless arc's color is a pseudocolor */
            if (*(*(*nfa).cm).cd.add((*a).co as usize)).flags & PSEUDO == 0 {
                return SATISFIED;
            }
        } else if (*a).co == RAINBOW {
            /* con is incompatible if it's for a pseudocolor */
            /* (this is hypothetical; we make no such constraints today) */
            if (*(*(*nfa).cm).cd.add((*con).co as usize)).flags & PSEUDO != 0 {
                return INCOMPATIBLE;
            }
            /* otherwise, constraint constrains arc to be only its color */
            return REPLACEARC;
        }
        return INCOMPATIBLE;
    }
    if key == CA(b'^' as c_int, b'^' as c_int) || key == CA(b'$' as c_int, b'$' as c_int) {
        /* collision, similar constraints */
        if (*con).co == (*a).co {
            /* true duplication */
            return SATISFIED;
        }
        return INCOMPATIBLE;
    }
    if key == CA(AHEAD, AHEAD) || key == CA(BEHIND, BEHIND) {
        /* collision, similar constraints */
        if (*con).co == (*a).co {
            /* true duplication */
            return SATISFIED;
        }
        if (*con).co == RAINBOW {
            /* con is satisfied unless arc's color is a pseudocolor */
            if (*(*(*nfa).cm).cd.add((*a).co as usize)).flags & PSEUDO == 0 {
                return SATISFIED;
            }
        } else if (*a).co == RAINBOW {
            /* con is incompatible if it's for a pseudocolor */
            /* (this is hypothetical; we make no such constraints today) */
            if (*(*(*nfa).cm).cd.add((*con).co as usize)).flags & PSEUDO != 0 {
                return INCOMPATIBLE;
            }
            /* otherwise, constraint constrains arc to be only its color */
            return REPLACEARC;
        }
        return INCOMPATIBLE;
    }
    if key == CA(b'^' as c_int, BEHIND)
        || key == CA(BEHIND, b'^' as c_int)
        || key == CA(b'$' as c_int, AHEAD)
        || key == CA(AHEAD, b'$' as c_int)
    {
        /* collision, dissimilar constraints */
        return INCOMPATIBLE;
    }
    if key == CA(b'^' as c_int, b'$' as c_int)
        || key == CA(b'^' as c_int, AHEAD)
        || key == CA(BEHIND, b'$' as c_int)
        || key == CA(BEHIND, AHEAD)
        || key == CA(b'$' as c_int, b'^' as c_int)
        || key == CA(b'$' as c_int, BEHIND)
        || key == CA(AHEAD, b'^' as c_int)
        || key == CA(AHEAD, BEHIND)
        || key == CA(b'^' as c_int, LACON)
        || key == CA(BEHIND, LACON)
        || key == CA(b'$' as c_int, LACON)
        || key == CA(AHEAD, LACON)
    {
        /* constraints passing each other */
        return COMPATIBLE;
    }

    Assert!(NOTREACHED != 0);
    INCOMPATIBLE /* for benefit of blind compilers */
}

/*
 * fixempties - get rid of EMPTY arcs
 */
pub unsafe fn fixempties(
    nfa: *mut nfa,
    f: *mut c_void, /* FILE *; for debug output; NULL none */
) {
    let mut s: *mut state;
    let mut s2: *mut state;
    let mut nexts: *mut state;
    let mut a: *mut arc;
    let mut nexta: *mut arc;
    let mut totalinarcs: c_int;
    let inarcsorig: *mut *mut arc;
    let arcarray: *mut *mut arc;
    let mut arccount: c_int;
    let mut prevnins: c_int;
    let mut nskip: c_int;

    /*
     * First, get rid of any states whose sole out-arc is an EMPTY, since
     * they're basically just aliases for their successor.  The parsing
     * algorithm creates enough of these that it's worth special-casing this.
     */
    s = (*nfa).states;
    while !s.is_null() && !NISERR(nfa) {
        nexts = (*s).next;
        if (*s).flag != 0 || (*s).nouts != 1 {
            s = nexts;
            continue;
        }
        a = (*s).outs;
        Assert!(!a.is_null() && (*a).outchain.is_null());
        if (*a).r#type != EMPTY {
            s = nexts;
            continue;
        }
        if s != (*a).to {
            moveins(nfa, s, (*a).to);
        }
        dropstate(nfa, s);
        s = nexts;
    }

    /*
     * Similarly, get rid of any state with a single EMPTY in-arc, by folding
     * it into its predecessor.
     */
    s = (*nfa).states;
    while !s.is_null() && !NISERR(nfa) {
        nexts = (*s).next;
        /* while we're at it, ensure tmp fields are clear for next step */
        Assert!((*s).tmp.is_null());
        if (*s).flag != 0 || (*s).nins != 1 {
            s = nexts;
            continue;
        }
        a = (*s).ins;
        Assert!(!a.is_null() && (*a).inchain.is_null());
        if (*a).r#type != EMPTY {
            s = nexts;
            continue;
        }
        if s != (*a).from {
            moveouts(nfa, s, (*a).from);
        }
        dropstate(nfa, s);
        s = nexts;
    }

    if NISERR(nfa) {
        return;
    }

    /*
     * For each remaining NFA state, find all other states from which it is
     * reachable by a chain of one or more EMPTY arcs.  Then generate new arcs
     * that eliminate the need for each such chain.
     *
     * We could replace a chain of EMPTY arcs that leads from a "from" state
     * to a "to" state either by pushing non-EMPTY arcs forward (linking
     * directly from "from"'s predecessors to "to") or by pulling them back
     * (linking directly from "from" to "to"'s successors).  We choose to
     * always do the former; this choice is somewhat arbitrary, but the
     * approach below requires that we uniformly do one or the other.
     *
     * Suppose we have a chain of N successive EMPTY arcs (where N can easily
     * approach the size of the NFA).  All of the intermediate states must
     * have additional inarcs and outarcs, else they'd have been removed by
     * the steps above.  Assuming their inarcs are mostly not empties, we will
     * add O(N^2) arcs to the NFA, since a non-EMPTY inarc leading to any one
     * state in the chain must be duplicated to lead to all its successor
     * states as well.  So there is no hope of doing less than O(N^2) work;
     * however, we should endeavor to keep the big-O cost from being even
     * worse than that, which it can easily become without care.  In
     * particular, suppose we were to copy all S1's inarcs forward to S2, and
     * then also to S3, and then later we consider pushing S2's inarcs forward
     * to S3.  If we include the arcs already copied from S1 in that, we'd be
     * doing O(N^3) work.  (The duplicate-arc elimination built into newarc()
     * and its cohorts would get rid of the extra arcs, but not without cost.)
     *
     * We can avoid this cost by treating only arcs that existed at the start
     * of this phase as candidates to be pushed forward.  To identify those,
     * we remember the first inarc each state had to start with.  We rely on
     * the fact that newarc() and friends put new arcs on the front of their
     * to-states' inchains, and that this phase never deletes arcs, so that
     * the original arcs must be the last arcs in their to-states' inchains.
     *
     * So the process here is that, for each state in the NFA, we gather up
     * all non-EMPTY inarcs of states that can reach the target state via
     * EMPTY arcs.  We then sort, de-duplicate, and merge these arcs into the
     * target state's inchain.  (We can safely use sort-merge for this as long
     * as we update each state's original-arcs pointer after we add arcs to
     * it; the sort step of mergeins probably changed the order of the old
     * arcs.)
     *
     * Another refinement worth making is that, because we only add non-EMPTY
     * arcs during this phase, and all added arcs have the same from-state as
     * the non-EMPTY arc they were cloned from, we know ahead of time that any
     * states having only EMPTY outarcs will be useless for lack of outarcs
     * after we drop the EMPTY arcs.  (They cannot gain non-EMPTY outarcs if
     * they had none to start with.)  So we need not bother to update the
     * inchains of such states at all.
     */

    /* Remember the states' first original inarcs */
    /* ... and while at it, count how many old inarcs there are altogether */
    inarcsorig =
        MALLOC((*nfa).nstates as usize * core::mem::size_of::<*mut arc>()) as *mut *mut arc;
    if inarcsorig.is_null() {
        NERR(nfa, REG_ESPACE);
        return;
    }
    totalinarcs = 0;
    s = (*nfa).states;
    while !s.is_null() {
        *inarcsorig.add((*s).no as usize) = (*s).ins;
        totalinarcs += (*s).nins;
        s = (*s).next;
    }

    /*
     * Create a workspace for accumulating the inarcs to be added to the
     * current target state.  totalinarcs is probably a considerable
     * overestimate of the space needed, but the NFA is unlikely to be large
     * enough at this point to make it worth being smarter.
     */
    arcarray = MALLOC(totalinarcs as usize * core::mem::size_of::<*mut arc>()) as *mut *mut arc;
    if arcarray.is_null() {
        NERR(nfa, REG_ESPACE);
        FREE(inarcsorig as *mut c_void);
        return;
    }

    /* And iterate over the target states */
    s = (*nfa).states;
    while !s.is_null() && !NISERR(nfa) {
        /* Ignore target states without non-EMPTY outarcs, per note above */
        if (*s).flag == 0 && hasnonemptyout(s) == 0 {
            s = (*s).next;
            continue;
        }

        /* Find predecessor states and accumulate their original inarcs */
        arccount = 0;
        s2 = emptyreachable(nfa, s, s, inarcsorig);
        while s2 != s {
            /* Add s2's original inarcs to arcarray[], but ignore empties */
            a = *inarcsorig.add((*s2).no as usize);
            while !a.is_null() {
                if (*a).r#type != EMPTY {
                    *arcarray.add(arccount as usize) = a;
                    arccount += 1;
                }
                a = (*a).inchain;
            }

            /* Reset the tmp fields as we walk back */
            nexts = (*s2).tmp;
            (*s2).tmp = null_mut();
            s2 = nexts;
        }
        (*s).tmp = null_mut();
        Assert!(arccount <= totalinarcs);

        /* Remember how many original inarcs this state has */
        prevnins = (*s).nins;

        /* Add non-duplicate inarcs to target state */
        mergeins(nfa, s, arcarray, arccount);

        /* Now we must update the state's inarcsorig pointer */
        nskip = (*s).nins - prevnins;
        a = (*s).ins;
        while nskip > 0 {
            a = (*a).inchain;
            nskip -= 1;
        }
        *inarcsorig.add((*s).no as usize) = a;

        s = (*s).next;
    }

    FREE(arcarray as *mut c_void);
    FREE(inarcsorig as *mut c_void);

    if NISERR(nfa) {
        return;
    }

    /*
     * Now remove all the EMPTY arcs, since we don't need them anymore.
     */
    s = (*nfa).states;
    while !s.is_null() {
        a = (*s).outs;
        while !a.is_null() {
            nexta = (*a).outchain;
            if (*a).r#type == EMPTY {
                freearc(nfa, a);
            }
            a = nexta;
        }
        s = (*s).next;
    }

    /*
     * And remove any states that have become useless.  (This cleanup is not
     * very thorough, and would be even less so if we tried to combine it with
     * the previous step; but cleanup() will take care of anything we miss.)
     */
    s = (*nfa).states;
    while !s.is_null() {
        nexts = (*s).next;
        if ((*s).nins == 0 || (*s).nouts == 0) && (*s).flag == 0 {
            dropstate(nfa, s);
        }
        s = nexts;
    }

    if !f.is_null() {
        dumpnfa(nfa, f);
    }
}

/*
 * emptyreachable - recursively find all states that can reach s by EMPTY arcs
 *
 * The return value is the last such state found.  Its tmp field links back
 * to the next-to-last such state, and so on back to s, so that all these
 * states can be located without searching the whole NFA.
 *
 * Since this is only used in fixempties(), we pass in the inarcsorig[] array
 * maintained by that function.  This lets us skip over all new inarcs, which
 * are certainly not EMPTY arcs.
 *
 * The maximum recursion depth here is equal to the length of the longest
 * loop-free chain of EMPTY arcs, which is surely no more than the size of
 * the NFA ... but that could still be enough to cause trouble.
 */
pub unsafe fn emptyreachable(
    nfa: *mut nfa,
    s: *mut state,
    lastfound: *mut state,
    inarcsorig: *mut *mut arc,
) -> *mut state {
    let mut a: *mut arc;
    let mut lastfound = lastfound;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return lastfound;
    }

    (*s).tmp = lastfound;
    lastfound = s;
    a = *inarcsorig.add((*s).no as usize);
    while !a.is_null() {
        if (*a).r#type == EMPTY && (*(*a).from).tmp.is_null() {
            lastfound = emptyreachable(nfa, (*a).from, lastfound, inarcsorig);
        }
        a = (*a).inchain;
    }
    lastfound
}

/*
 * isconstraintarc - detect whether an arc is of a constraint type
 */
#[inline]
unsafe fn isconstraintarc(a: *mut arc) -> c_int {
    match (*a).r#type {
        t if t == b'^' as c_int
            || t == b'$' as c_int
            || t == BEHIND
            || t == AHEAD
            || t == LACON =>
        {
            1
        }
        _ => 0,
    }
}

/*
 * hasconstraintout - does state have a constraint out arc?
 */
pub unsafe fn hasconstraintout(s: *mut state) -> c_int {
    let mut a: *mut arc;

    a = (*s).outs;
    while !a.is_null() {
        if isconstraintarc(a) != 0 {
            return 1;
        }
        a = (*a).outchain;
    }
    0
}

/*
 * fixconstraintloops - get rid of loops containing only constraint arcs
 *
 * A loop of states that contains only constraint arcs is useless, since
 * passing around the loop represents no forward progress.  Moreover, it
 * would cause infinite looping in pullback/pushfwd, so we need to get rid
 * of such loops before doing that.
 */
pub unsafe fn fixconstraintloops(
    nfa: *mut nfa,
    f: *mut c_void, /* FILE *; for debug output; NULL none */
) {
    let mut s: *mut state;
    let mut nexts: *mut state;
    let mut a: *mut arc;
    let mut nexta: *mut arc;
    let mut hasconstraints: c_int;

    /*
     * In the trivial case of a state that loops to itself, we can just drop
     * the constraint arc altogether.  This is worth special-casing because
     * such loops are far more common than loops containing multiple states.
     * While we're at it, note whether any constraint arcs survive.
     */
    hasconstraints = 0;
    s = (*nfa).states;
    while !s.is_null() && !NISERR(nfa) {
        nexts = (*s).next;
        /* while we're at it, ensure tmp fields are clear for next step */
        Assert!((*s).tmp.is_null());
        a = (*s).outs;
        while !a.is_null() && !NISERR(nfa) {
            nexta = (*a).outchain;
            if isconstraintarc(a) != 0 {
                if (*a).to == s {
                    freearc(nfa, a);
                } else {
                    hasconstraints = 1;
                }
            }
            a = nexta;
        }
        /* If we removed all the outarcs, the state is useless. */
        if (*s).nouts == 0 && (*s).flag == 0 {
            dropstate(nfa, s);
        }
        s = nexts;
    }

    /* Nothing to do if no remaining constraint arcs */
    if NISERR(nfa) || hasconstraints == 0 {
        return;
    }

    /*
     * Starting from each remaining NFA state, search outwards for a
     * constraint loop.  If we find a loop, break the loop, then start the
     * search over.  (We could possibly retain some state from the first scan,
     * but it would complicate things greatly, and multi-state constraint
     * loops are rare enough that it's not worth optimizing the case.)
     */
    'restart: loop {
        s = (*nfa).states;
        while !s.is_null() && !NISERR(nfa) {
            if findconstraintloop(nfa, s) != 0 {
                continue 'restart;
            }
            s = (*s).next;
        }
        break;
    }

    if NISERR(nfa) {
        return;
    }

    /*
     * Now remove any states that have become useless.  (This cleanup is not
     * very thorough, and would be even less so if we tried to combine it with
     * the previous step; but cleanup() will take care of anything we miss.)
     *
     * Because findconstraintloop intentionally doesn't reset all tmp fields,
     * we have to clear them after it's done.  This is a convenient place to
     * do that, too.
     */
    s = (*nfa).states;
    while !s.is_null() {
        nexts = (*s).next;
        (*s).tmp = null_mut();
        if ((*s).nins == 0 || (*s).nouts == 0) && (*s).flag == 0 {
            dropstate(nfa, s);
        }
        s = nexts;
    }

    if !f.is_null() {
        dumpnfa(nfa, f);
    }
}

/*
 * findconstraintloop - recursively find a loop of constraint arcs
 *
 * If we find a loop, break it by calling breakconstraintloop(), then
 * return 1; otherwise return 0.
 *
 * State tmp fields are guaranteed all NULL on a success return, because
 * breakconstraintloop does that.  After a failure return, any state that
 * is known not to be part of a loop is marked with s->tmp == s; this allows
 * us not to have to re-prove that fact on later calls.  (This convention is
 * workable because we already eliminated single-state loops.)
 *
 * Note that the found loop doesn't necessarily include the first state we
 * are called on.  Any loop reachable from that state will do.
 *
 * The maximum recursion depth here is one more than the length of the longest
 * loop-free chain of constraint arcs, which is surely no more than the size
 * of the NFA ... but that could still be enough to cause trouble.
 */
pub unsafe fn findconstraintloop(nfa: *mut nfa, s: *mut state) -> c_int {
    let mut a: *mut arc;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return 1; /* to exit as quickly as possible */
    }

    if !(*s).tmp.is_null() {
        /* Already proven uninteresting? */
        if (*s).tmp == s {
            return 0;
        }
        /* Found a loop involving s */
        breakconstraintloop(nfa, s);
        /* The tmp fields have been cleaned up by breakconstraintloop */
        return 1;
    }
    a = (*s).outs;
    while !a.is_null() {
        if isconstraintarc(a) != 0 {
            let sto: *mut state = (*a).to;

            Assert!(sto != s);
            (*s).tmp = sto;
            if findconstraintloop(nfa, sto) != 0 {
                return 1;
            }
        }
        a = (*a).outchain;
    }

    /*
     * If we get here, no constraint loop exists leading out from s.  Mark it
     * with s->tmp == s so we need not rediscover that fact again later.
     */
    (*s).tmp = s;
    0
}

/*
 * breakconstraintloop - break a loop of constraint arcs
 *
 * sinitial is any one member state of the loop.  Each loop member's tmp
 * field links to its successor within the loop.  (Note that this function
 * will reset all the tmp fields to NULL.)
 *
 * We can break the loop by, for any one state S1 in the loop, cloning its
 * loop successor state S2 (and possibly following states), and then moving
 * all S1->S2 constraint arcs to point to the cloned S2.  The cloned S2 should
 * copy any non-constraint outarcs of S2.  Constraint outarcs should be
 * dropped if they point back to S1, else they need to be copied as arcs to
 * similarly cloned states S3, S4, etc.  In general, each cloned state copies
 * non-constraint outarcs, drops constraint outarcs that would lead to itself
 * or any earlier cloned state, and sends other constraint outarcs to newly
 * cloned states.  No cloned state will have any inarcs that aren't constraint
 * arcs or do not lead from S1 or earlier-cloned states.  It's okay to drop
 * constraint back-arcs since they would not take us to any state we've not
 * already been in; therefore, no new constraint loop is created.  In this way
 * we generate a modified NFA that can still represent every useful state
 * sequence, but not sequences that represent state loops with no consumption
 * of input data.  Note that the set of cloned states will certainly include
 * all of the loop member states other than S1, and it may also include
 * non-loop states that are reachable from S2 via constraint arcs.  This is
 * important because there is no guarantee that findconstraintloop found a
 * maximal loop (and searching for one would be NP-hard, so don't try).
 * Frequently the "non-loop states" are actually part of a larger loop that
 * we didn't notice, and indeed there may be several overlapping loops.
 * This technique ensures convergence in such cases, while considering only
 * the originally-found loop does not.
 *
 * If there is only one S1->S2 constraint arc, then that constraint is
 * certainly satisfied when we enter any of the clone states.  This means that
 * in the common case where many of the constraint arcs are identically
 * labeled, we can merge together clone states linked by a similarly-labeled
 * constraint: if we can get to the first one we can certainly get to the
 * second, so there's no need to distinguish.  This greatly reduces the number
 * of new states needed, so we preferentially break the given loop at a state
 * pair where this is true.
 *
 * Furthermore, it's fairly common to find that a cloned successor state has
 * no outarcs, especially if we're a bit aggressive about removing unnecessary
 * outarcs.  If that happens, then there is simply not any interesting state
 * that can be reached through the predecessor's loop arcs, which means we can
 * break the loop just by removing those loop arcs, with no new states added.
 */
pub unsafe fn breakconstraintloop(nfa: *mut nfa, sinitial: *mut state) {
    let mut s: *mut state;
    let shead: *mut state;
    let stail: *mut state;
    let mut sclone: *mut state;
    let mut nexts: *mut state;
    let mut refarc: *mut arc;
    let mut a: *mut arc;
    let mut nexta: *mut arc;

    /*
     * Start by identifying which loop step we want to break at.
     * Preferentially this is one with only one constraint arc.  (XXX are
     * there any other secondary heuristics we want to use here?)  Set refarc
     * to point to the selected lone constraint arc, if there is one.
     */
    refarc = null_mut();
    s = sinitial;
    loop {
        nexts = (*s).tmp;
        Assert!(nexts != s); /* should not see any one-element loops */
        if refarc.is_null() {
            let mut narcs: c_int = 0;

            a = (*s).outs;
            while !a.is_null() {
                if (*a).to == nexts && isconstraintarc(a) != 0 {
                    refarc = a;
                    narcs += 1;
                }
                a = (*a).outchain;
            }
            Assert!(narcs > 0);
            if narcs > 1 {
                refarc = null_mut(); /* multiple constraint arcs here, no good */
            }
        }
        s = nexts;
        if s == sinitial {
            break;
        }
    }

    if !refarc.is_null() {
        /* break at the refarc */
        shead = (*refarc).from;
        stail = (*refarc).to;
        Assert!(stail == (*shead).tmp);
    } else {
        /* for lack of a better idea, break after sinitial */
        shead = sinitial;
        stail = (*sinitial).tmp;
    }

    /*
     * Reset the tmp fields so that we can use them for local storage in
     * clonesuccessorstates.  (findconstraintloop won't mind, since it's just
     * going to abandon its search anyway.)
     */
    s = (*nfa).states;
    while !s.is_null() {
        (*s).tmp = null_mut();
        s = (*s).next;
    }

    /*
     * Recursively build clone state(s) as needed.
     */
    sclone = newstate(nfa);
    if sclone.is_null() {
        Assert!(NISERR(nfa));
        return;
    }

    clonesuccessorstates(
        nfa,
        stail,
        sclone,
        shead,
        refarc,
        null_mut(),
        null_mut(),
        (*nfa).nstates,
    );

    if NISERR(nfa) {
        return;
    }

    /*
     * It's possible that sclone has no outarcs at all, in which case it's
     * useless.  (We don't try extremely hard to get rid of useless states
     * here, but this is an easy and fairly common case.)
     */
    if (*sclone).nouts == 0 {
        freestate(nfa, sclone);
        sclone = null_mut();
    }

    /*
     * Move shead's constraint-loop arcs to point to sclone, or just drop them
     * if we discovered we don't need sclone.
     */
    a = (*shead).outs;
    while !a.is_null() {
        nexta = (*a).outchain;
        if (*a).to == stail && isconstraintarc(a) != 0 {
            if !sclone.is_null() {
                cparc(nfa, a, shead, sclone);
            }
            freearc(nfa, a);
            if NISERR(nfa) {
                break;
            }
        }
        a = nexta;
    }
}

/*
 * clonesuccessorstates - create a tree of constraint-arc successor states
 *
 * ssource is the state to be cloned, and sclone is the state to copy its
 * outarcs into.  sclone's inarcs, if any, should already be set up.
 *
 * spredecessor is the original predecessor state that we are trying to build
 * successors for (it may not be the immediate predecessor of ssource).
 * refarc, if not NULL, is the original constraint arc that is known to have
 * been traversed out of spredecessor to reach the successor(s).
 *
 * For each cloned successor state, we transiently create a "donemap" that is
 * a boolean array showing which source states we've already visited for this
 * clone state.  This prevents infinite recursion as well as useless repeat
 * visits to the same state subtree (which can add up fast, since typical NFAs
 * have multiple redundant arc pathways).  Each donemap is a char array
 * indexed by state number.  The donemaps are all of the same size "nstates",
 * which is nfa->nstates as of the start of the recursion.  This is enough to
 * have entries for all pre-existing states, but *not* entries for clone
 * states created during the recursion.  That's okay since we have no need to
 * mark those.
 *
 * curdonemap is NULL when recursing to a new sclone state, or sclone's
 * donemap when we are recursing without having created a new state (which we
 * do when we decide we can merge a successor state into the current clone
 * state).  outerdonemap is NULL at the top level and otherwise the parent
 * clone state's donemap.
 *
 * The successor states we create and fill here form a strict tree structure,
 * with each state having exactly one predecessor, except that the toplevel
 * state has no inarcs as yet (breakconstraintloop will add its inarcs from
 * spredecessor after we're done).  Thus, we can examine sclone's inarcs back
 * to the root, plus refarc if any, to identify the set of constraints already
 * known valid at the current point.  This allows us to avoid generating extra
 * successor states.
 */
pub unsafe fn clonesuccessorstates(
    nfa: *mut nfa,
    ssource: *mut state,
    sclone: *mut state,
    spredecessor: *mut state,
    refarc: *mut arc,
    curdonemap: *mut c_char,
    outerdonemap: *mut c_char,
    nstates: c_int,
) {
    let donemap: *mut c_char;
    let mut a: *mut arc;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return;
    }

    /* If this state hasn't already got a donemap, create one */
    donemap = if curdonemap.is_null() {
        let dm = MALLOC(nstates as usize * core::mem::size_of::<c_char>()) as *mut c_char;
        if dm.is_null() {
            NERR(nfa, REG_ESPACE);
            return;
        }

        if !outerdonemap.is_null() {
            /*
             * Not at outermost recursion level, so copy the outer level's
             * donemap; this ensures that we see states in process of being
             * visited at outer levels, or already merged into predecessor
             * states, as ones we shouldn't traverse back to.
             */
            core::ptr::copy_nonoverlapping(
                outerdonemap,
                dm,
                nstates as usize * core::mem::size_of::<c_char>(),
            );
        } else {
            /* At outermost level, only spredecessor is off-limits */
            core::ptr::write_bytes(dm, 0, nstates as usize * core::mem::size_of::<c_char>());
            Assert!((*spredecessor).no < nstates);
            *dm.add((*spredecessor).no as usize) = 1;
        }
        dm
    } else {
        curdonemap
    };

    /* Mark ssource as visited in the donemap */
    Assert!((*ssource).no < nstates);
    Assert!(*donemap.add((*ssource).no as usize) == 0);
    *donemap.add((*ssource).no as usize) = 1;

    /*
     * We proceed by first cloning all of ssource's outarcs, creating new
     * clone states as needed but not doing more with them than that.  Then in
     * a second pass, recurse to process the child clone states.  This allows
     * us to have only one child clone state per reachable source state, even
     * when there are multiple outarcs leading to the same state.  Also, when
     * we do visit a child state, its set of inarcs is known exactly, which
     * makes it safe to apply the constraint-is-already-checked optimization.
     * Also, this ensures that we've merged all the states we can into the
     * current clone before we recurse to any children, thus possibly saving
     * them from making extra images of those states.
     *
     * While this function runs, child clone states of the current state are
     * marked by setting their tmp fields to point to the original state they
     * were cloned from.  This makes it possible to detect multiple outarcs
     * leading to the same state, and also makes it easy to distinguish clone
     * states from original states (which will have tmp == NULL).
     */
    a = (*ssource).outs;
    while !a.is_null() && !NISERR(nfa) {
        let sto: *mut state = (*a).to;

        /*
         * We do not consider cloning successor states that have no constraint
         * outarcs; just link to them as-is.  They cannot be part of a
         * constraint loop so there is no need to make copies.  In particular,
         * this rule keeps us from trying to clone the post state, which would
         * be a bad idea.
         */
        if isconstraintarc(a) != 0 && hasconstraintout(sto) != 0 {
            let mut prevclone: *mut state;
            let mut canmerge: c_int;
            let mut a2: *mut arc;

            /*
             * Back-link constraint arcs must not be followed.  Nor is there a
             * need to revisit states previously merged into this clone.
             */
            Assert!((*sto).no < nstates);
            if *donemap.add((*sto).no as usize) != 0 {
                a = (*a).outchain;
                continue;
            }

            /*
             * Check whether we already have a child clone state for this
             * source state.
             */
            prevclone = null_mut();
            a2 = (*sclone).outs;
            while !a2.is_null() {
                if (*(*a2).to).tmp == sto {
                    prevclone = (*a2).to;
                    break;
                }
                a2 = (*a2).outchain;
            }

            /*
             * If this arc is labeled the same as refarc, or the same as any
             * arc we must have traversed to get to sclone, then no additional
             * constraints need to be met to get to sto, so we should just
             * merge its outarcs into sclone.
             */
            if !refarc.is_null() && (*a).r#type == (*refarc).r#type && (*a).co == (*refarc).co {
                canmerge = 1;
            } else {
                let mut s: *mut state;

                canmerge = 0;
                s = sclone;
                while !(*s).ins.is_null() {
                    if (*s).nins == 1
                        && (*a).r#type == (*(*s).ins).r#type
                        && (*a).co == (*(*s).ins).co
                    {
                        canmerge = 1;
                        break;
                    }
                    s = (*(*s).ins).from;
                }
            }

            if canmerge != 0 {
                /*
                 * We can merge into sclone.  If we previously made a child
                 * clone state, drop it; there's no need to visit it.  (This
                 * can happen if ssource has multiple pathways to sto, and we
                 * only just now found one that is provably a no-op.)
                 */
                if !prevclone.is_null() {
                    dropstate(nfa, prevclone); /* kills our outarc, too */
                }

                /* Recurse to merge sto's outarcs into sclone */
                clonesuccessorstates(
                    nfa,
                    sto,
                    sclone,
                    spredecessor,
                    refarc,
                    donemap,
                    outerdonemap,
                    nstates,
                );
                /* sto should now be marked as previously visited */
                Assert!(NISERR(nfa) || *donemap.add((*sto).no as usize) == 1);
            } else if !prevclone.is_null() {
                /*
                 * We already have a clone state for this successor, so just
                 * make another arc to it.
                 */
                cparc(nfa, a, sclone, prevclone);
            } else {
                /*
                 * We need to create a new successor clone state.
                 */
                let stoclone: *mut state;

                stoclone = newstate(nfa);
                if stoclone.is_null() {
                    Assert!(NISERR(nfa));
                    break;
                }
                /* Mark it as to what it's a clone of */
                (*stoclone).tmp = sto;
                /* ... and add the outarc leading to it */
                cparc(nfa, a, sclone, stoclone);
            }
        } else {
            /*
             * Non-constraint outarcs just get copied to sclone, as do outarcs
             * leading to states with no constraint outarc.
             */
            cparc(nfa, a, sclone, sto);
        }
        a = (*a).outchain;
    }

    /*
     * If we are at outer level for this clone state, recurse to all its child
     * clone states, clearing their tmp fields as we go.  (If we're not
     * outermost for sclone, leave this to be done by the outer call level.)
     * Note that if we have multiple outarcs leading to the same clone state,
     * it will only be recursed-to once.
     */
    if curdonemap.is_null() {
        a = (*sclone).outs;
        while !a.is_null() && !NISERR(nfa) {
            let stoclone: *mut state = (*a).to;
            let sto: *mut state = (*stoclone).tmp;

            if !sto.is_null() {
                (*stoclone).tmp = null_mut();
                clonesuccessorstates(
                    nfa,
                    sto,
                    stoclone,
                    spredecessor,
                    refarc,
                    null_mut(),
                    donemap,
                    nstates,
                );
            }
            a = (*a).outchain;
        }

        /* Don't forget to free sclone's donemap when done with it */
        FREE(donemap as *mut c_void);
    }
}

/*
 * removecantmatch - remove CANTMATCH arcs, which are no longer useful
 * once we are done with the parsing phase.  (We need them only to
 * preserve connectedness of NFA subgraphs during parsing.)
 */
pub unsafe fn removecantmatch(nfa: *mut nfa) {
    let mut s: *mut state;

    s = (*nfa).states;
    while !s.is_null() {
        let mut a: *mut arc;
        let mut nexta: *mut arc;

        a = (*s).outs;
        while !a.is_null() {
            nexta = (*a).outchain;
            if (*a).r#type == CANTMATCH {
                freearc(nfa, a);
                if NISERR(nfa) {
                    return;
                }
            }
            a = nexta;
        }
        s = (*s).next;
    }
}

/*
 * cleanup - clean up NFA after optimizations
 */
pub unsafe fn cleanup(nfa: *mut nfa) {
    let mut s: *mut state;
    let mut nexts: *mut state;
    let mut n: c_int;

    if NISERR(nfa) {
        return;
    }

    /* clear out unreachable or dead-end states */
    /* use pre to mark reachable, then post to mark can-reach-post */
    markreachable(nfa, (*nfa).pre, null_mut(), (*nfa).pre);
    markcanreach(nfa, (*nfa).post, (*nfa).pre, (*nfa).post);
    s = (*nfa).states;
    while !s.is_null() && !NISERR(nfa) {
        nexts = (*s).next;
        if (*s).tmp != (*nfa).post && (*s).flag == 0 {
            dropstate(nfa, s);
        }
        s = nexts;
    }
    Assert!(NISERR(nfa) || (*(*nfa).post).nins == 0 || (*(*nfa).post).tmp == (*nfa).post);
    cleartraverse(nfa, (*nfa).pre);
    Assert!(NISERR(nfa) || (*(*nfa).post).nins == 0 || (*(*nfa).post).tmp.is_null());
    /* the nins==0 (final unreachable) case will be caught later */

    /* renumber surviving states */
    n = 0;
    s = (*nfa).states;
    while !s.is_null() {
        (*s).no = n;
        n += 1;
        s = (*s).next;
    }
    (*nfa).nstates = n;
}

/*
 * markreachable - recursive marking of reachable states
 */
pub unsafe fn markreachable(
    nfa: *mut nfa,
    s: *mut state,
    okay: *mut state, /* consider only states with this mark */
    mark: *mut state, /* the value to mark with */
) {
    let mut a: *mut arc;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return;
    }

    if (*s).tmp != okay {
        return;
    }
    (*s).tmp = mark;

    a = (*s).outs;
    while !a.is_null() {
        markreachable(nfa, (*a).to, okay, mark);
        a = (*a).outchain;
    }
}

/*
 * markcanreach - recursive marking of states which can reach here
 */
pub unsafe fn markcanreach(
    nfa: *mut nfa,
    s: *mut state,
    okay: *mut state, /* consider only states with this mark */
    mark: *mut state, /* the value to mark with */
) {
    let mut a: *mut arc;

    /* Since this is recursive, it could be driven to stack overflow */
    if STACK_TOO_DEEP(nfa) {
        NERR(nfa, REG_ETOOBIG);
        return;
    }

    if (*s).tmp != okay {
        return;
    }
    (*s).tmp = mark;

    a = (*s).ins;
    while !a.is_null() {
        markcanreach(nfa, (*a).from, okay, mark);
        a = (*a).inchain;
    }
}

/*
 * analyze - ascertain potentially-useful facts about an optimized NFA
 */
pub unsafe fn analyze(nfa: *mut nfa) -> c_long /* re_info bits to be ORed in */ {
    let mut a: *mut arc;
    let mut aa: *mut arc;

    if NISERR(nfa) {
        return 0;
    }

    /* Detect whether NFA can't match anything */
    if (*(*nfa).pre).outs.is_null() {
        return REG_UIMPOSSIBLE as c_long;
    }

    /* Detect whether NFA matches all strings (possibly with length bounds) */
    checkmatchall(nfa);

    /* Detect whether NFA can possibly match a zero-length string */
    a = (*(*nfa).pre).outs;
    while !a.is_null() {
        aa = (*(*a).to).outs;
        while !aa.is_null() {
            if (*aa).to == (*nfa).post {
                return REG_UEMPTYMATCH as c_long;
            }
            aa = (*aa).outchain;
        }
        a = (*a).outchain;
    }
    0
}

/*
 * checkmatchall - does the NFA represent no more than a string length test?
 *
 * If so, set nfa->minmatchall and nfa->maxmatchall correctly (they are -1
 * to begin with) and set the MATCHALL bit in nfa->flags.
 *
 * To succeed, we require all arcs to be PLAIN RAINBOW arcs, except for those
 * for pseudocolors (i.e., BOS/BOL/EOS/EOL).  We must be able to reach the
 * post state via RAINBOW arcs, and if there are any loops in the graph, they
 * must be loop-to-self arcs, ensuring that each loop iteration consumes
 * exactly one character.  (Longer loops are problematic because they create
 * non-consecutive possible match lengths; we have no good way to represent
 * that situation for lengths beyond the DUPINF limit.)
 *
 * Pseudocolor arcs complicate things a little.  We know that they can only
 * appear as pre-state outarcs (for BOS/BOL) or post-state inarcs (for
 * EOS/EOL).  There, they must exactly replicate the parallel RAINBOW arcs,
 * e.g. if the pre state has one RAINBOW outarc to state 2, it must have BOS
 * and BOL outarcs to state 2, and no others.  Missing or extra pseudocolor
 * arcs can occur, meaning that the NFA involves some constraint on the
 * adjacent characters, which makes it not a matchall NFA.
 */
pub unsafe fn checkmatchall(nfa: *mut nfa) {
    let haspaths: *mut *mut bool;
    let mut s: *mut state;
    let mut i: c_int;

    /*
     * If there are too many states, don't bother trying to detect matchall.
     * This limit serves to bound the time and memory we could consume below.
     * Note that even if the graph is all-RAINBOW, if there are significantly
     * more than DUPINF states then it's likely that there are paths of length
     * more than DUPINF, which would force us to fail anyhow.  In practice,
     * plausible ways of writing a matchall regex with maximum finite path
     * length K tend not to have very many more than K states.
     */
    if (*nfa).nstates > DUPINF * 2 {
        return;
    }

    /*
     * First, scan all the states to verify that only RAINBOW arcs appear,
     * plus pseudocolor arcs adjacent to the pre and post states.  This lets
     * us quickly eliminate most cases that aren't matchall NFAs.
     */
    s = (*nfa).states;
    while !s.is_null() {
        let mut a: *mut arc;

        a = (*s).outs;
        while !a.is_null() {
            if (*a).r#type != PLAIN {
                return; /* any LACONs make it non-matchall */
            }
            if (*a).co != RAINBOW {
                if (*(*(*nfa).cm).cd.add((*a).co as usize)).flags & PSEUDO != 0 {
                    /*
                     * Pseudocolor arc: verify it's in a valid place (this
                     * seems quite unlikely to fail, but let's be sure).
                     */
                    if s == (*nfa).pre && ((*a).co == (*nfa).bos[0] || (*a).co == (*nfa).bos[1]) {
                        /* okay BOS/BOL arc */
                    } else if (*a).to == (*nfa).post
                        && ((*a).co == (*nfa).eos[0] || (*a).co == (*nfa).eos[1])
                    {
                        /* okay EOS/EOL arc */
                    } else {
                        return; /* unexpected pseudocolor arc */
                    }
                    /* We'll check these arcs some more below. */
                } else {
                    return; /* any other color makes it non-matchall */
                }
            }
            a = (*a).outchain;
        }
        /* Also, assert that the tmp fields are available for use. */
        Assert!((*s).tmp.is_null());
        s = (*s).next;
    }

    /*
     * The next cheapest check we can make is to verify that the BOS/BOL
     * outarcs of the pre state reach the same states as its RAINBOW outarcs.
     * If they don't, the NFA expresses some constraints on the character
     * before the matched string, making it non-matchall.  Likewise, the
     * EOS/EOL inarcs of the post state must match its RAINBOW inarcs.
     */
    if !check_out_colors_match((*nfa).pre, RAINBOW, (*nfa).bos[0])
        || !check_out_colors_match((*nfa).pre, RAINBOW, (*nfa).bos[1])
        || !check_in_colors_match((*nfa).post, RAINBOW, (*nfa).eos[0])
        || !check_in_colors_match((*nfa).post, RAINBOW, (*nfa).eos[1])
    {
        return;
    }

    /*
     * Initialize an array of path-length arrays, in which
     * checkmatchall_recurse will return per-state results.  This lets us
     * memo-ize the recursive search and avoid exponential time consumption.
     */
    haspaths = MALLOC((*nfa).nstates as usize * core::mem::size_of::<*mut bool>()) as *mut *mut bool;
    if haspaths.is_null() {
        return; /* fail quietly */
    }
    core::ptr::write_bytes(
        haspaths,
        0,
        (*nfa).nstates as usize * core::mem::size_of::<*mut bool>(),
    );

    /*
     * Recursively search the graph for all-RAINBOW paths to the "post" state,
     * starting at the "pre" state, and computing the lengths of the paths.
     * (Given the preceding checks, there should be at least one such path.
     * However we could get back a false result anyway, in case there are
     * multi-state loops, paths exceeding DUPINF+1 length, or non-algorithmic
     * failures such as ENOMEM.)
     */
    if checkmatchall_recurse(nfa, (*nfa).pre, haspaths) {
        /* The useful result is the path length array for the pre state */
        let mut haspath: *mut bool = *haspaths.add((*(*nfa).pre).no as usize);
        let mut minmatch: c_int;
        let mut maxmatch: c_int;
        let mut morematch: c_int;

        Assert!(!haspath.is_null());

        /*
         * haspath[] now represents the set of possible path lengths; but we
         * want to reduce that to a min and max value, because it doesn't seem
         * worth complicating regexec.c to deal with nonconsecutive possible
         * match lengths.  Find min and max of first run of lengths, then
         * verify there are no nonconsecutive lengths.
         */
        minmatch = 0;
        while minmatch <= DUPINF + 1 {
            if *haspath.add(minmatch as usize) {
                break;
            }
            minmatch += 1;
        }
        Assert!(minmatch <= DUPINF + 1); /* else checkmatchall_recurse lied */
        maxmatch = minmatch;
        while maxmatch < DUPINF + 1 {
            if !*haspath.add((maxmatch + 1) as usize) {
                break;
            }
            maxmatch += 1;
        }
        morematch = maxmatch + 1;
        while morematch <= DUPINF + 1 {
            if *haspath.add(morematch as usize) {
                haspath = null_mut(); /* fail, there are nonconsecutive lengths */
                break;
            }
            morematch += 1;
        }

        if !haspath.is_null() {
            /*
             * Success, so record the info.  Here we have a fine point: the
             * path length from the pre state includes the pre-to-initial
             * transition, so it's one more than the actually matched string
             * length.  (We avoided counting the final-to-post transition
             * within checkmatchall_recurse, but not this one.)  This is why
             * checkmatchall_recurse allows one more level of path length than
             * might seem necessary.  This decrement also takes care of
             * converting checkmatchall_recurse's definition of "infinity" as
             * "DUPINF+1" to our normal representation as "DUPINF".
             */
            Assert!(minmatch > 0); /* else pre and post states were adjacent */
            (*nfa).minmatchall = minmatch - 1;
            (*nfa).maxmatchall = maxmatch - 1;
            (*nfa).flags |= MATCHALL;
        }
    }

    /* Clean up */
    i = 0;
    while i < (*nfa).nstates {
        if !(*haspaths.add(i as usize)).is_null() {
            FREE(*haspaths.add(i as usize) as *mut c_void);
        }
        i += 1;
    }
    FREE(haspaths as *mut c_void);
}

/*
 * checkmatchall_recurse - recursive search for checkmatchall
 *
 * s is the state to be examined in this recursion level.
 * haspaths[] is an array of per-state exit path length arrays.
 *
 * We return true if the search was performed successfully, false if
 * we had to fail because of multi-state loops or other internal reasons.
 * (Because "dead" states that can't reach the post state have been
 * eliminated, and we already verified that only RAINBOW and matching
 * pseudocolor arcs exist, every state should have RAINBOW path(s) to
 * the post state.  Hence we take a false result from recursive calls
 * as meaning that we'd better fail altogether, not just that that
 * particular state can't reach the post state.)
 *
 * On success, we store a malloc'd result array in haspaths[s->no],
 * showing the possible path lengths from s to the post state.
 * Each state's haspath[] array is of length DUPINF+2.  The entries from
 * k = 0 to DUPINF are true if there is an all-RAINBOW path of length k
 * from this state to the string end.  haspath[DUPINF+1] is true if all
 * path lengths >= DUPINF+1 are possible.  (Situations that cannot be
 * represented under these rules cause failure.)
 *
 * checkmatchall is responsible for eventually freeing the haspath[] arrays.
 */
pub unsafe fn checkmatchall_recurse(nfa: *mut nfa, s: *mut state, haspaths: *mut *mut bool) -> bool {
    let mut result: bool = false;
    let mut foundloop: bool = false;
    let haspath: *mut bool;
    let mut a: *mut arc;

    /*
     * Since this is recursive, it could be driven to stack overflow.  But we
     * need not treat that as a hard failure; just deem the NFA non-matchall.
     */
    if STACK_TOO_DEEP(nfa) {
        return false;
    }

    /* In case the search takes a long time, check for cancel */
    INTERRUPT((*((*nfa).v as *mut vars)).re);

    /* Create a haspath array for this state */
    haspath = MALLOC((DUPINF + 2) as usize * core::mem::size_of::<bool>()) as *mut bool;
    if haspath.is_null() {
        return false; /* again, treat as non-matchall */
    }
    core::ptr::write_bytes(
        haspath,
        0,
        (DUPINF + 2) as usize * core::mem::size_of::<bool>(),
    );

    /* Mark this state as being visited */
    Assert!((*s).tmp.is_null());
    (*s).tmp = s;

    a = (*s).outs;
    while !a.is_null() {
        if (*a).co != RAINBOW {
            a = (*a).outchain;
            continue; /* ignore pseudocolor arcs */
        }
        if (*a).to == (*nfa).post {
            /* We found an all-RAINBOW path to the post state */
            result = true;

            /*
             * Mark this state as being zero steps away from the string end
             * (the transition to the post state isn't counted).
             */
            *haspath.add(0) = true;
        } else if (*a).to == s {
            /* We found a cycle of length 1, which we'll deal with below. */
            foundloop = true;
        } else if !(*(*a).to).tmp.is_null() {
            /* It's busy, so we found a cycle of length > 1, so fail. */
            result = false;
            break;
        } else {
            /* Consider paths forward through this to-state. */
            let nexthaspath: *mut bool;
            let mut i: c_int;

            /* If to-state was not already visited, recurse */
            if (*haspaths.add((*(*a).to).no as usize)).is_null() {
                result = checkmatchall_recurse(nfa, (*a).to, haspaths);
                /* Fail if any recursive path fails */
                if !result {
                    break;
                }
            } else {
                /* The previous visit must have found path(s) to the end */
                result = true;
            }
            Assert!((*(*a).to).tmp.is_null());
            nexthaspath = *haspaths.add((*(*a).to).no as usize);
            Assert!(!nexthaspath.is_null());

            /*
             * Now, for every path of length i from a->to to the string end,
             * there is a path of length i + 1 from s to the string end.
             */
            if *nexthaspath.add(DUPINF as usize) != *nexthaspath.add((DUPINF + 1) as usize) {
                /*
                 * a->to has a path of length exactly DUPINF, but not longer;
                 * or it has paths of all lengths > DUPINF but not one of
                 * exactly that length.  In either case, we cannot represent
                 * the possible path lengths from s correctly, so fail.
                 */
                result = false;
                break;
            }
            /* Merge knowledge of these path lengths into what we have */
            i = 0;
            while i < DUPINF {
                *haspath.add((i + 1) as usize) =
                    *haspath.add((i + 1) as usize) || *nexthaspath.add(i as usize);
                i += 1;
            }
            /* Infinity + 1 is still infinity */
            *haspath.add((DUPINF + 1) as usize) = *haspath.add((DUPINF + 1) as usize)
                || *nexthaspath.add((DUPINF + 1) as usize);
        }
        a = (*a).outchain;
    }

    if result && foundloop {
        /*
         * If there is a length-1 loop at this state, then find the shortest
         * known path length to the end.  The loop means that every larger
         * path length is possible, too.  (It doesn't matter whether any of
         * the longer lengths were already known possible.)
         */
        let mut i: c_int;

        i = 0;
        while i <= DUPINF {
            if *haspath.add(i as usize) {
                break;
            }
            i += 1;
        }
        i += 1;
        while i <= DUPINF + 1 {
            *haspath.add(i as usize) = true;
            i += 1;
        }
    }

    /* Report out the completed path length map */
    Assert!((*s).no < (*nfa).nstates);
    Assert!((*haspaths.add((*s).no as usize)).is_null());
    *haspaths.add((*s).no as usize) = haspath;

    /* Mark state no longer busy */
    (*s).tmp = null_mut();

    result
}

/*
 * check_out_colors_match - subroutine for checkmatchall
 *
 * Check whether the set of states reachable from s by arcs of color co1
 * is equivalent to the set reachable by arcs of color co2.
 * checkmatchall already verified that all of the NFA's arcs are PLAIN,
 * so we need not examine arc types here.
 */
pub unsafe fn check_out_colors_match(s: *mut state, co1: color, co2: color) -> bool {
    let mut result: bool = true;
    let mut a: *mut arc;

    /*
     * To do this in linear time, we assume that the NFA contains no duplicate
     * arcs.  Run through the out-arcs, marking states reachable by arcs of
     * color co1.  Run through again, un-marking states reachable by arcs of
     * color co2; if we see a not-marked state, we know this co2 arc is
     * unmatched.  Then run through again, checking for still-marked states,
     * and in any case leaving all the tmp fields reset to NULL.
     */
    a = (*s).outs;
    while !a.is_null() {
        if (*a).co == co1 {
            Assert!((*(*a).to).tmp.is_null());
            (*(*a).to).tmp = (*a).to;
        }
        a = (*a).outchain;
    }
    a = (*s).outs;
    while !a.is_null() {
        if (*a).co == co2 {
            if !(*(*a).to).tmp.is_null() {
                (*(*a).to).tmp = null_mut();
            } else {
                result = false; /* unmatched co2 arc */
            }
        }
        a = (*a).outchain;
    }
    a = (*s).outs;
    while !a.is_null() {
        if (*a).co == co1 {
            if !(*(*a).to).tmp.is_null() {
                result = false; /* unmatched co1 arc */
                (*(*a).to).tmp = null_mut();
            }
        }
        a = (*a).outchain;
    }
    result
}

/*
 * check_in_colors_match - subroutine for checkmatchall
 *
 * Check whether the set of states that can reach s by arcs of color co1
 * is equivalent to the set that can reach s by arcs of color co2.
 * checkmatchall already verified that all of the NFA's arcs are PLAIN,
 * so we need not examine arc types here.
 */
pub unsafe fn check_in_colors_match(s: *mut state, co1: color, co2: color) -> bool {
    let mut result: bool = true;
    let mut a: *mut arc;

    /*
     * Identical algorithm to check_out_colors_match, except examine the
     * from-states of s' inarcs.
     */
    a = (*s).ins;
    while !a.is_null() {
        if (*a).co == co1 {
            Assert!((*(*a).from).tmp.is_null());
            (*(*a).from).tmp = (*a).from;
        }
        a = (*a).inchain;
    }
    a = (*s).ins;
    while !a.is_null() {
        if (*a).co == co2 {
            if !(*(*a).from).tmp.is_null() {
                (*(*a).from).tmp = null_mut();
            } else {
                result = false; /* unmatched co2 arc */
            }
        }
        a = (*a).inchain;
    }
    a = (*s).ins;
    while !a.is_null() {
        if (*a).co == co1 {
            if !(*(*a).from).tmp.is_null() {
                result = false; /* unmatched co1 arc */
                (*(*a).from).tmp = null_mut();
            }
        }
        a = (*a).inchain;
    }
    result
}

/*
 * compact - construct the compact representation of an NFA
 */
pub unsafe fn compact(nfa: *mut nfa, cnfa: *mut cnfa) {
    let mut s: *mut state;
    let mut a: *mut arc;
    let mut nstates: Size;
    let mut narcs: Size;
    let mut ca: *mut crate::regex::regguts::carc;
    let mut first: *mut crate::regex::regguts::carc;

    Assert!(!NISERR(nfa));

    nstates = 0;
    narcs = 0;
    s = (*nfa).states;
    while !s.is_null() {
        nstates += 1;
        narcs += ((*s).nouts + 1) as Size; /* need one extra for endmarker */
        s = (*s).next;
    }

    (*cnfa).stflags = MALLOC(nstates * core::mem::size_of::<c_char>()) as *mut c_char;
    (*cnfa).states = MALLOC(nstates * core::mem::size_of::<*mut crate::regex::regguts::carc>())
        as *mut *mut crate::regex::regguts::carc;
    (*cnfa).arcs = MALLOC(narcs * core::mem::size_of::<crate::regex::regguts::carc>())
        as *mut crate::regex::regguts::carc;
    if (*cnfa).stflags.is_null() || (*cnfa).states.is_null() || (*cnfa).arcs.is_null() {
        if !(*cnfa).stflags.is_null() {
            FREE((*cnfa).stflags as *mut c_void);
        }
        if !(*cnfa).states.is_null() {
            FREE((*cnfa).states as *mut c_void);
        }
        if !(*cnfa).arcs.is_null() {
            FREE((*cnfa).arcs as *mut c_void);
        }
        NERR(nfa, REG_ESPACE);
        return;
    }
    (*cnfa).nstates = nstates as c_int;
    (*cnfa).pre = (*(*nfa).pre).no;
    (*cnfa).post = (*(*nfa).post).no;
    (*cnfa).bos[0] = (*nfa).bos[0];
    (*cnfa).bos[1] = (*nfa).bos[1];
    (*cnfa).eos[0] = (*nfa).eos[0];
    (*cnfa).eos[1] = (*nfa).eos[1];
    (*cnfa).ncolors = maxcolor((*nfa).cm) as c_int + 1;
    (*cnfa).flags = (*nfa).flags;
    (*cnfa).minmatchall = (*nfa).minmatchall;
    (*cnfa).maxmatchall = (*nfa).maxmatchall;

    ca = (*cnfa).arcs;
    s = (*nfa).states;
    while !s.is_null() {
        Assert!(((*s).no as Size) < nstates);
        *(*cnfa).stflags.add((*s).no as usize) = 0;
        *(*cnfa).states.add((*s).no as usize) = ca;
        first = ca;
        a = (*s).outs;
        while !a.is_null() {
            match (*a).r#type {
                t if t == PLAIN => {
                    (*ca).co = (*a).co;
                    (*ca).to = (*(*a).to).no;
                    ca = ca.add(1);
                }
                t if t == LACON => {
                    Assert!((*s).no != (*cnfa).pre);
                    Assert!((*a).co >= 0);
                    (*ca).co = ((*cnfa).ncolors + (*a).co as c_int) as color;
                    (*ca).to = (*(*a).to).no;
                    ca = ca.add(1);
                    (*cnfa).flags |= HASLACONS;
                }
                _ => {
                    NERR(nfa, REG_ASSERT);
                    return;
                }
            }
            a = (*a).outchain;
        }
        carcsort(first, ca.offset_from(first) as Size);
        (*ca).co = COLORLESS;
        (*ca).to = 0;
        ca = ca.add(1);
        s = (*s).next;
    }
    Assert!(ca == (*cnfa).arcs.add(narcs));
    Assert!((*cnfa).nstates != 0);

    /* mark no-progress states */
    a = (*(*nfa).pre).outs;
    while !a.is_null() {
        *(*cnfa).stflags.add((*(*a).to).no as usize) = CNFA_NOPROGRESS as c_char;
        a = (*a).outchain;
    }
    *(*cnfa).stflags.add((*(*nfa).pre).no as usize) = CNFA_NOPROGRESS as c_char;
}

/*
 * carcsort - sort compacted-NFA arcs by color
 */
pub unsafe fn carcsort(first: *mut crate::regex::regguts::carc, n: Size) {
    if n > 1 {
        pg_qsort(
            first as *mut c_void,
            n as usize,
            core::mem::size_of::<crate::regex::regguts::carc>(),
            carc_cmp,
        );
    }
}

unsafe fn carc_cmp(a: *const c_void, b: *const c_void) -> c_int {
    let aa: *const crate::regex::regguts::carc = a as *const crate::regex::regguts::carc;
    let bb: *const crate::regex::regguts::carc = b as *const crate::regex::regguts::carc;

    if (*aa).co < (*bb).co {
        return -1;
    }
    if (*aa).co > (*bb).co {
        return 1;
    }
    if (*aa).to < (*bb).to {
        return -1;
    }
    if (*aa).to > (*bb).to {
        return 1;
    }
    /* This is unreached, since there should be no duplicate arcs now: */
    0
}

/*
 * freecnfa - free a compacted NFA
 */
pub unsafe fn freecnfa(cnfa: *mut cnfa) {
    Assert!(!NULLCNFA(&*cnfa)); /* not empty already */
    FREE((*cnfa).stflags as *mut c_void);
    FREE((*cnfa).states as *mut c_void);
    FREE((*cnfa).arcs as *mut c_void);
    ZAPCNFA(&mut *cnfa);
}

/*
 * dumpnfa - dump an NFA in human-readable form
 */
pub unsafe fn dumpnfa(nfa: *mut nfa, f: *mut c_void) {
    let _ = (nfa, f);
    // #ifdef REG_DEBUG -- body compiled out when REG_DEBUG is undefined.
}

// #ifdef REG_DEBUG -- subordinates of dumpnfa

/*
 * dumpstate - dump an NFA state in human-readable form
 */
pub unsafe fn dumpstate(s: *mut state, f: *mut c_void) {
    let _ = (s, f);
    // #ifdef REG_DEBUG -- body compiled out when REG_DEBUG is undefined.
}

/*
 * dumparcs - dump out-arcs in human-readable form
 */
pub unsafe fn dumparcs(s: *mut state, f: *mut c_void) {
    let _ = (s, f);
    // #ifdef REG_DEBUG -- body compiled out when REG_DEBUG is undefined.
}

/*
 * dumparc - dump one outarc in readable form, including prefixing tab
 */
pub unsafe fn dumparc(a: *mut arc, s: *mut state, f: *mut c_void) {
    let _ = (a, s, f);
    // #ifdef REG_DEBUG -- body compiled out when REG_DEBUG is undefined.
}

/*
 * dumpcnfa - dump a compacted NFA in human-readable form
 */
pub unsafe fn dumpcnfa(cnfa: *mut cnfa, f: *mut c_void) {
    let _ = (cnfa, f);
    // #ifdef REG_DEBUG -- body compiled out when REG_DEBUG is undefined.
}

// #ifdef REG_DEBUG -- subordinates of dumpcnfa

/*
 * dumpcstate - dump a compacted-NFA state in human-readable form
 */
pub unsafe fn dumpcstate(st: c_int, cnfa: *mut cnfa, f: *mut c_void) {
    let _ = (st, cnfa, f);
    // #ifdef REG_DEBUG -- body compiled out when REG_DEBUG is undefined.
}
