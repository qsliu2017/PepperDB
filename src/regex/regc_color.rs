//! regex/regc_color.c - colorings of characters.
//!
//! This file is #included by regcomp.c. Copyright (c) 1998, 1999 Henry Spencer.
//! See PostgreSQL source for the full license text. Manages the compile-time
//! color machinery and its NFA-arc maintenance.
//!
//! Note that there are some incestuous relationships between this code and
//! NFA arc maintenance, which perhaps ought to be cleaned up sometime.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use core::ffi::c_void;

use crate::c::Size;
use crate::regex::regcustom::{chr, CHR_MIN, MAX_SIMPLE_CHR};
use crate::regex::regerror::{REG_ECOLORS, REG_ESPACE};
use crate::regex::regex::regex_t;
use crate::regex::regguts::{
    arc, color, colordesc, colormap, colormaprange, cvec, nfa, state, subre, CDEND, CMMAGIC,
    COLMARK, COLORLESS, FREECOL, MAX_COLOR, NINLINECDS, NOSUB, PSEUDO, RAINBOW, UNUSEDCOLOR,
    WHITE,
};
use core::ffi::c_int;

// regc_color.c is #included into regcomp.c, so it shares regcomp.c's
// MALLOC/FREE/REALLOC, the struct vars layout, and the EOS/PLAIN/CANTMATCH arc
// type codes, plus the NFA arc routines newarc()/findarc() from regc_nfa.c.
// regcomp.c isn't fully ported yet, so mirror the pieces we need here.
use crate::regex::regc_nfa::{findarc, newarc};
use crate::utils::palloc::{palloc_extended, pfree, MCXT_ALLOC_NO_OOM};

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

/// end of string token type (regcomp.c #define EOS), used by VERR.
const EOS: c_int = b'e' as c_int;
/// ordinary character arc type (regcomp.c #define PLAIN).
pub const PLAIN: c_int = b'p' as c_int;
/// arc that cannot match anything (regcomp.c #define CANTMATCH).
pub const CANTMATCH: c_int = b'x' as c_int;

// struct vars (defined in regcomp.c, not a header). regc_color.c reaches into
// it via cm->v; mirror the faithful layout here.  regguts::vars is an opaque
// c_void, so callers cast.
// TODO(pg-port): unify with regcomp.c's struct vars once that file is ported.
use crate::regex::regcomp::vars;

use crate::regex::regc_locale::cclass_column_index;

// #define CISERR()	VISERR(cm->v)  ;  VISERR(vv) is ((vv)->err != 0)
#[inline]
unsafe fn CISERR(cm: *mut colormap) -> bool {
    (*((*cm).v as *mut vars)).err != 0
}

// #define CERR(e)		VERR(cm->v, (e))
// VERR(vv,e) is ((vv)->nexttype = EOS, (vv)->err = ((vv)->err ? (vv)->err : (e)))
#[inline]
unsafe fn CERR(cm: *mut colormap, e: c_int) {
    let vv = (*cm).v as *mut vars;
    (*vv).nexttype = EOS;
    (*vv).err = if (*vv).err != 0 { (*vv).err } else { e };
}

/// #define NOERR() {if (ISERR()) return;}  -- ISERR() is VISERR(v).
macro_rules! NOERR {
    ($v:expr) => {
        if (*($v)).err != 0 {
            return;
        }
    };
}

/*
 * initcm - set up new colormap
 */
pub unsafe fn initcm(v: *mut vars, cm: *mut colormap) {
    let cd: *mut colordesc;

    (*cm).magic = CMMAGIC;
    (*cm).v = v as *mut c_void;

    (*cm).ncds = NINLINECDS;
    (*cm).cd = (*cm).cdspace.as_mut_ptr();
    (*cm).max = 0;
    (*cm).free = 0;

    cd = (*cm).cd; /* cm->cd[WHITE] */
    (*cd).nschrs = (MAX_SIMPLE_CHR - CHR_MIN + 1) as c_int;
    (*cd).nuchrs = 1;
    (*cd).sub = NOSUB;
    (*cd).arcs = std::ptr::null_mut();
    (*cd).firstchr = CHR_MIN;
    (*cd).flags = 0;

    (*cm).locolormap = MALLOC(
        ((MAX_SIMPLE_CHR - CHR_MIN + 1) as usize) * std::mem::size_of::<color>(),
    ) as *mut color;
    if (*cm).locolormap.is_null() {
        CERR(cm, REG_ESPACE);
        (*cm).cmranges = std::ptr::null_mut(); /* prevent failure during freecm */
        (*cm).hicolormap = std::ptr::null_mut();
        return;
    }
    /* this memset relies on WHITE being zero: */
    std::ptr::write_bytes(
        (*cm).locolormap as *mut u8,
        WHITE as u8,
        ((MAX_SIMPLE_CHR - CHR_MIN + 1) as usize) * std::mem::size_of::<color>(),
    );

    std::ptr::write_bytes(
        (*cm).classbits.as_mut_ptr() as *mut u8,
        0,
        std::mem::size_of_val(&(*cm).classbits),
    );
    (*cm).numcmranges = 0;
    (*cm).cmranges = std::ptr::null_mut();
    (*cm).maxarrayrows = 4; /* arbitrary initial allocation */
    (*cm).hiarrayrows = 1; /* but we have only one row/col initially */
    (*cm).hiarraycols = 1;
    (*cm).hicolormap =
        MALLOC((*cm).maxarrayrows as usize * std::mem::size_of::<color>()) as *mut color;
    if (*cm).hicolormap.is_null() {
        CERR(cm, REG_ESPACE);
        return;
    }
    /* initialize the "all other characters" row to WHITE */
    *(*cm).hicolormap.add(0) = WHITE;
}

/*
 * freecm - free dynamically-allocated things in a colormap
 */
pub unsafe fn freecm(cm: *mut colormap) {
    (*cm).magic = 0;
    if (*cm).cd != (*cm).cdspace.as_mut_ptr() {
        FREE((*cm).cd as *mut c_void);
    }
    if !(*cm).locolormap.is_null() {
        FREE((*cm).locolormap as *mut c_void);
    }
    if !(*cm).cmranges.is_null() {
        FREE((*cm).cmranges as *mut c_void);
    }
    if !(*cm).hicolormap.is_null() {
        FREE((*cm).hicolormap as *mut c_void);
    }
}

/*
 * pg_reg_getcolor - slow case of GETCOLOR()
 */
pub unsafe fn pg_reg_getcolor(cm: *mut colormap, c: chr) -> color {
    let mut rownum: c_int;
    let colnum: c_int;
    let mut low: c_int;
    let mut high: c_int;

    /* Should not be used for chrs in the locolormap */
    assert!(c > MAX_SIMPLE_CHR);

    /*
     * Find which row it's in.  The colormapranges are in order, so we can use
     * binary search.
     */
    rownum = 0; /* if no match, use array row zero */
    low = 0;
    high = (*cm).numcmranges;
    while low < high {
        let middle = low + (high - low) / 2;
        let cmr: *const colormaprange = (*cm).cmranges.add(middle as usize);

        if c < (*cmr).cmin {
            high = middle;
        } else if c > (*cmr).cmax {
            low = middle + 1;
        } else {
            rownum = (*cmr).rownum; /* found a match */
            break;
        }
    }

    /*
     * Find which column it's in --- this is all locale-dependent.
     */
    if (*cm).hiarraycols > 1 {
        colnum = cclass_column_index(cm, c);
        *(*cm)
            .hicolormap
            .add((rownum * (*cm).hiarraycols + colnum) as usize)
    } else {
        /* fast path if no relevant cclasses */
        *(*cm).hicolormap.add(rownum as usize)
    }
}

/*
 * maxcolor - report largest color number in use
 */
pub unsafe fn maxcolor(cm: *mut colormap) -> color {
    if CISERR(cm) {
        return COLORLESS;
    }

    (*cm).max as color
}

/*
 * newcolor - find a new color (must be assigned at once)
 * Beware:	may relocate the colordescs.
 */
pub unsafe fn newcolor(cm: *mut colormap) -> color /* COLORLESS for error */ {
    let cd: *mut colordesc;
    let mut n: usize;

    if CISERR(cm) {
        return COLORLESS;
    }

    if (*cm).free != 0 {
        assert!((*cm).free > 0);
        assert!(((*cm).free as usize) < (*cm).ncds);
        cd = (*cm).cd.add((*cm).free as usize);
        assert!(UNUSEDCOLOR(cd) != 0);
        assert!((*cd).arcs.is_null());
        (*cm).free = (*cd).sub;
    } else if (*cm).max < (*cm).ncds - 1 {
        (*cm).max += 1;
        cd = (*cm).cd.add((*cm).max);
    } else {
        /* oops, must allocate more */
        let newCd: *mut colordesc;

        if (*cm).max as c_int == MAX_COLOR {
            CERR(cm, REG_ECOLORS);
            return COLORLESS; /* too many colors */
        }

        n = (*cm).ncds * 2;
        if n > (MAX_COLOR + 1) as usize {
            n = (MAX_COLOR + 1) as usize;
        }
        if (*cm).cd == (*cm).cdspace.as_mut_ptr() {
            newCd = MALLOC(n * std::mem::size_of::<colordesc>()) as *mut colordesc;
            if !newCd.is_null() {
                std::ptr::copy_nonoverlapping(
                    (*cm).cdspace.as_ptr(),
                    newCd,
                    (*cm).ncds,
                );
            }
        } else {
            newCd = REALLOC(
                (*cm).cd as *mut c_void,
                n * std::mem::size_of::<colordesc>(),
            ) as *mut colordesc;
        }
        if newCd.is_null() {
            CERR(cm, REG_ESPACE);
            return COLORLESS;
        }
        (*cm).cd = newCd;
        (*cm).ncds = n;
        assert!((*cm).max < (*cm).ncds - 1);
        (*cm).max += 1;
        cd = (*cm).cd.add((*cm).max);
    }

    (*cd).nschrs = 0;
    (*cd).nuchrs = 0;
    (*cd).sub = NOSUB;
    (*cd).arcs = std::ptr::null_mut();
    (*cd).firstchr = CHR_MIN; /* in case never set otherwise */
    (*cd).flags = 0;

    (cd.offset_from((*cm).cd)) as color
}

/*
 * freecolor - free a color (must have no arcs or subcolor)
 */
pub unsafe fn freecolor(cm: *mut colormap, co: color) {
    let cd: *mut colordesc = (*cm).cd.add(co as usize);
    let mut pco: color;
    let mut nco: color; /* for freelist scan */

    assert!(co >= 0);
    if co == WHITE {
        return;
    }

    assert!((*cd).arcs.is_null());
    assert!((*cd).sub == NOSUB);
    assert!((*cd).nschrs == 0);
    assert!((*cd).nuchrs == 0);
    (*cd).flags = FREECOL;

    if (co as usize) == (*cm).max {
        while (*cm).max > WHITE as usize && UNUSEDCOLOR((*cm).cd.add((*cm).max)) != 0 {
            (*cm).max -= 1;
        }
        assert!((*cm).free >= 0);
        while ((*cm).free as usize) > (*cm).max {
            (*cm).free = (*(*cm).cd.add((*cm).free as usize)).sub;
        }
        if (*cm).free > 0 {
            assert!(((*cm).free as usize) < (*cm).max);
            pco = (*cm).free;
            nco = (*(*cm).cd.add(pco as usize)).sub;
            while nco > 0 {
                if (nco as usize) > (*cm).max {
                    /* take this one out of freelist */
                    nco = (*(*cm).cd.add(nco as usize)).sub;
                    (*(*cm).cd.add(pco as usize)).sub = nco;
                } else {
                    assert!((nco as usize) < (*cm).max);
                    pco = nco;
                    nco = (*(*cm).cd.add(pco as usize)).sub;
                }
            }
        }
    } else {
        (*cd).sub = (*cm).free;
        (*cm).free = (cd.offset_from((*cm).cd)) as color;
    }
}

/*
 * pseudocolor - allocate a false color, to be managed by other means
 */
pub unsafe fn pseudocolor(cm: *mut colormap) -> color {
    let co: color;
    let cd: *mut colordesc;

    co = newcolor(cm);
    if CISERR(cm) {
        return COLORLESS;
    }
    cd = (*cm).cd.add(co as usize);
    (*cd).nschrs = 0;
    (*cd).nuchrs = 1; /* pretend it is in the upper map */
    (*cd).sub = NOSUB;
    (*cd).arcs = std::ptr::null_mut();
    (*cd).firstchr = CHR_MIN;
    (*cd).flags = PSEUDO;
    co
}

/*
 * subcolor - allocate a new subcolor (if necessary) to this chr
 *
 * This works only for chrs that map into the low color map.
 */
pub unsafe fn subcolor(cm: *mut colormap, c: chr) -> color {
    let co: color; /* current color of c */
    let sco: color; /* new subcolor */

    assert!(c <= MAX_SIMPLE_CHR);

    co = *(*cm).locolormap.add((c - CHR_MIN) as usize);
    sco = newsub(cm, co);
    if CISERR(cm) {
        return COLORLESS;
    }
    assert!(sco != COLORLESS);

    if co == sco {
        /* already in an open subcolor */
        return co; /* rest is redundant */
    }
    (*(*cm).cd.add(co as usize)).nschrs -= 1;
    if (*(*cm).cd.add(sco as usize)).nschrs == 0 {
        (*(*cm).cd.add(sco as usize)).firstchr = c;
    }
    (*(*cm).cd.add(sco as usize)).nschrs += 1;
    *(*cm).locolormap.add((c - CHR_MIN) as usize) = sco;
    sco
}

/*
 * subcolorhi - allocate a new subcolor (if necessary) to this colormap entry
 *
 * This is the same processing as subcolor(), but for entries in the high
 * colormap, which do not necessarily correspond to exactly one chr code.
 */
pub unsafe fn subcolorhi(cm: *mut colormap, pco: *mut color) -> color {
    let co: color; /* current color of entry */
    let sco: color; /* new subcolor */

    co = *pco;
    sco = newsub(cm, co);
    if CISERR(cm) {
        return COLORLESS;
    }
    assert!(sco != COLORLESS);

    if co == sco {
        /* already in an open subcolor */
        return co; /* rest is redundant */
    }
    (*(*cm).cd.add(co as usize)).nuchrs -= 1;
    (*(*cm).cd.add(sco as usize)).nuchrs += 1;
    *pco = sco;
    sco
}

/*
 * newsub - allocate a new subcolor (if necessary) for a color
 */
pub unsafe fn newsub(cm: *mut colormap, co: color) -> color {
    let mut sco: color; /* new subcolor */

    sco = (*(*cm).cd.add(co as usize)).sub;
    if sco == NOSUB {
        /* color has no open subcolor */
        /* optimization: singly-referenced color need not be subcolored */
        if ((*(*cm).cd.add(co as usize)).nschrs + (*(*cm).cd.add(co as usize)).nuchrs) == 1 {
            return co;
        }
        sco = newcolor(cm); /* must create subcolor */
        if sco == COLORLESS {
            assert!(CISERR(cm));
            return COLORLESS;
        }
        (*(*cm).cd.add(co as usize)).sub = sco;
        (*(*cm).cd.add(sco as usize)).sub = sco; /* open subcolor points to self */
    }
    assert!(sco != NOSUB);

    sco
}

/*
 * newhicolorrow - get a new row in the hicolormap, cloning it from oldrow
 *
 * Returns array index of new row.  Note the array might move.
 */
pub unsafe fn newhicolorrow(cm: *mut colormap, oldrow: c_int) -> c_int {
    let newrow: c_int = (*cm).hiarrayrows;
    let newrowptr: *mut color;
    let mut i: c_int;

    /* Assign a fresh array row index, enlarging storage if needed */
    if newrow >= (*cm).maxarrayrows {
        let newarray: *mut color;

        if (*cm).maxarrayrows >= c_int::MAX / ((*cm).hiarraycols * 2) {
            CERR(cm, REG_ESPACE);
            return 0;
        }
        newarray = REALLOC(
            (*cm).hicolormap as *mut c_void,
            (*cm).maxarrayrows as usize
                * 2
                * (*cm).hiarraycols as usize
                * std::mem::size_of::<color>(),
        ) as *mut color;
        if newarray.is_null() {
            CERR(cm, REG_ESPACE);
            return 0;
        }
        (*cm).hicolormap = newarray;
        (*cm).maxarrayrows *= 2;
    }
    (*cm).hiarrayrows += 1;

    /* Copy old row data */
    newrowptr = (*cm).hicolormap.add((newrow * (*cm).hiarraycols) as usize);
    std::ptr::copy_nonoverlapping(
        (*cm).hicolormap.add((oldrow * (*cm).hiarraycols) as usize),
        newrowptr,
        (*cm).hiarraycols as usize,
    );

    /* Increase color reference counts to reflect new colormap entries */
    i = 0;
    while i < (*cm).hiarraycols {
        (*(*cm).cd.add(*newrowptr.add(i as usize) as usize)).nuchrs += 1;
        i += 1;
    }

    newrow
}

/*
 * newhicolorcols - create a new set of columns in the high colormap
 *
 * Essentially, extends the 2-D array to the right with a copy of itself.
 */
pub unsafe fn newhicolorcols(cm: *mut colormap) {
    let newarray: *mut color;
    let mut r: c_int;
    let mut c: c_int;

    if (*cm).hiarraycols >= c_int::MAX / ((*cm).maxarrayrows * 2) {
        CERR(cm, REG_ESPACE);
        return;
    }
    newarray = REALLOC(
        (*cm).hicolormap as *mut c_void,
        (*cm).maxarrayrows as usize
            * (*cm).hiarraycols as usize
            * 2
            * std::mem::size_of::<color>(),
    ) as *mut color;
    if newarray.is_null() {
        CERR(cm, REG_ESPACE);
        return;
    }
    (*cm).hicolormap = newarray;

    /* Duplicate existing columns to the right, and increase ref counts */
    /* Must work backwards in the array because we realloc'd in place */
    r = (*cm).hiarrayrows - 1;
    while r >= 0 {
        let oldrowptr: *mut color = newarray.add((r * (*cm).hiarraycols) as usize);
        let newrowptr: *mut color = newarray.add((r * (*cm).hiarraycols * 2) as usize);
        let newrowptr2: *mut color = newrowptr.add((*cm).hiarraycols as usize);

        c = 0;
        while c < (*cm).hiarraycols {
            let co: color = *oldrowptr.add(c as usize);

            *newrowptr.add(c as usize) = co;
            *newrowptr2.add(c as usize) = co;
            (*(*cm).cd.add(co as usize)).nuchrs += 1;
            c += 1;
        }
        r -= 1;
    }

    (*cm).hiarraycols *= 2;
}

/*
 * subcolorcvec - allocate new subcolors to cvec members, fill in arcs
 *
 * For each chr "c" represented by the cvec, do the equivalent of
 * newarc(v->nfa, PLAIN, subcolor(v->cm, c), lp, rp);
 *
 * Note that in typical cases, many of the subcolors are the same.
 * While newarc() would discard duplicate arc requests, we can save
 * some cycles by not calling it repetitively to begin with.  This is
 * mechanized with the "lastsubcolor" state variable.
 */
pub unsafe fn subcolorcvec(v: *mut vars, cv: *mut cvec, lp: *mut state, rp: *mut state) {
    let cm: *mut colormap = (*v).cm;
    let mut lastsubcolor: color = COLORLESS;
    let mut ch: chr;
    let mut from: chr;
    let mut to: chr;
    let mut p: *const chr;
    let mut i: c_int;

    /* ordinary characters */
    p = (*cv).chrs;
    i = (*cv).nchrs;
    while i > 0 {
        ch = *p;
        subcoloronechr(v, ch, lp, rp, &mut lastsubcolor);
        NOERR!(v);
        p = p.add(1);
        i -= 1;
    }

    /* and the ranges */
    p = (*cv).ranges;
    i = (*cv).nranges;
    while i > 0 {
        from = *p;
        to = *p.add(1);
        if from <= MAX_SIMPLE_CHR {
            /* deal with simple chars one at a time */
            let lim: chr = if to <= MAX_SIMPLE_CHR { to } else { MAX_SIMPLE_CHR };

            while from <= lim {
                let sco: color = subcolor(cm, from);

                NOERR!(v);
                if sco != lastsubcolor {
                    newarc((*v).nfa, PLAIN, sco, lp, rp);
                    NOERR!(v);
                    lastsubcolor = sco;
                }
                from += 1;
            }
        }
        /* deal with any part of the range that's above MAX_SIMPLE_CHR */
        if from < to {
            subcoloronerange(v, from, to, lp, rp, &mut lastsubcolor);
        } else if from == to {
            subcoloronechr(v, from, lp, rp, &mut lastsubcolor);
        }
        NOERR!(v);
        p = p.add(2);
        i -= 1;
    }

    /* and deal with cclass if any */
    if (*cv).cclasscode >= 0 {
        let classbit: c_int;
        let mut pco: *mut color;
        let mut r: c_int;
        let mut c: c_int;

        /* Enlarge array if we don't have a column bit assignment for cclass */
        if (*cm).classbits[(*cv).cclasscode as usize] == 0 {
            (*cm).classbits[(*cv).cclasscode as usize] = (*cm).hiarraycols;
            newhicolorcols(cm);
            NOERR!(v);
        }
        /* Apply subcolorhi() and make arc for each entry in relevant cols */
        classbit = (*cm).classbits[(*cv).cclasscode as usize];
        pco = (*cm).hicolormap;
        r = 0;
        while r < (*cm).hiarrayrows {
            c = 0;
            while c < (*cm).hiarraycols {
                if c & classbit != 0 {
                    let sco: color = subcolorhi(cm, pco);

                    NOERR!(v);
                    /* add the arc if needed */
                    if sco != lastsubcolor {
                        newarc((*v).nfa, PLAIN, sco, lp, rp);
                        NOERR!(v);
                        lastsubcolor = sco;
                    }
                }
                pco = pco.add(1);
                c += 1;
            }
            r += 1;
        }
    }
}

/*
 * subcoloronechr - do subcolorcvec's work for a singleton chr
 *
 * We could just let subcoloronerange do this, but it's a bit more efficient
 * if we exploit the single-chr case.  Also, callers find it useful for this
 * to be able to handle both low and high chr codes.
 */
pub unsafe fn subcoloronechr(
    v: *mut vars,
    ch: chr,
    lp: *mut state,
    rp: *mut state,
    lastsubcolor: *mut color,
) {
    let cm: *mut colormap = (*v).cm;
    let newranges: *mut colormaprange;
    let mut numnewranges: c_int;
    let mut oldrange: *mut colormaprange;
    let mut oldrangen: c_int;
    let newrow: c_int;

    /* Easy case for low chr codes */
    if ch <= MAX_SIMPLE_CHR {
        let sco: color = subcolor(cm, ch);

        NOERR!(v);
        if sco != *lastsubcolor {
            newarc((*v).nfa, PLAIN, sco, lp, rp);
            *lastsubcolor = sco;
        }
        return;
    }

    /*
     * Potentially, we could need two more colormapranges than we have now, if
     * the given chr is in the middle of some existing range.
     */
    newranges = MALLOC(
        ((*cm).numcmranges + 2) as usize * std::mem::size_of::<colormaprange>(),
    ) as *mut colormaprange;
    if newranges.is_null() {
        CERR(cm, REG_ESPACE);
        return;
    }
    numnewranges = 0;

    /* Ranges before target are unchanged */
    oldrange = (*cm).cmranges;
    oldrangen = 0;
    while oldrangen < (*cm).numcmranges {
        if (*oldrange).cmax >= ch {
            break;
        }
        *newranges.add(numnewranges as usize) = *oldrange;
        numnewranges += 1;
        oldrange = oldrange.add(1);
        oldrangen += 1;
    }

    /* Match target chr against current range */
    if oldrangen >= (*cm).numcmranges || (*oldrange).cmin > ch {
        /* chr does not belong to any existing range, make a new one */
        (*newranges.add(numnewranges as usize)).cmin = ch;
        (*newranges.add(numnewranges as usize)).cmax = ch;
        /* row state should be cloned from the "all others" row */
        newrow = newhicolorrow(cm, 0);
        (*newranges.add(numnewranges as usize)).rownum = newrow;
        numnewranges += 1;
    } else if (*oldrange).cmin == (*oldrange).cmax {
        /* we have an existing singleton range matching the chr */
        *newranges.add(numnewranges as usize) = *oldrange;
        numnewranges += 1;
        newrow = (*oldrange).rownum;
        /* we've now fully processed this old range */
        oldrange = oldrange.add(1);
        oldrangen += 1;
    } else {
        /* chr is a subset of this existing range, must split it */
        if ch > (*oldrange).cmin {
            /* emit portion of old range before chr */
            (*newranges.add(numnewranges as usize)).cmin = (*oldrange).cmin;
            (*newranges.add(numnewranges as usize)).cmax = ch - 1;
            (*newranges.add(numnewranges as usize)).rownum = (*oldrange).rownum;
            numnewranges += 1;
        }
        /* emit chr as singleton range, initially cloning from range */
        (*newranges.add(numnewranges as usize)).cmin = ch;
        (*newranges.add(numnewranges as usize)).cmax = ch;
        newrow = newhicolorrow(cm, (*oldrange).rownum);
        (*newranges.add(numnewranges as usize)).rownum = newrow;
        numnewranges += 1;
        if ch < (*oldrange).cmax {
            /* emit portion of old range after chr */
            (*newranges.add(numnewranges as usize)).cmin = ch + 1;
            (*newranges.add(numnewranges as usize)).cmax = (*oldrange).cmax;
            /* must clone the row if we are making two new ranges from old */
            (*newranges.add(numnewranges as usize)).rownum = if ch > (*oldrange).cmin {
                newhicolorrow(cm, (*oldrange).rownum)
            } else {
                (*oldrange).rownum
            };
            numnewranges += 1;
        }
        /* we've now fully processed this old range */
        oldrange = oldrange.add(1);
        oldrangen += 1;
    }

    /* Update colors in newrow and create arcs as needed */
    subcoloronerow(v, newrow, lp, rp, lastsubcolor);

    /* Ranges after target are unchanged */
    while oldrangen < (*cm).numcmranges {
        *newranges.add(numnewranges as usize) = *oldrange;
        numnewranges += 1;
        oldrange = oldrange.add(1);
        oldrangen += 1;
    }

    /* Assert our original space estimate was adequate */
    assert!(numnewranges <= ((*cm).numcmranges + 2));

    /* And finally, store back the updated list of ranges */
    if !(*cm).cmranges.is_null() {
        FREE((*cm).cmranges as *mut c_void);
    }
    (*cm).cmranges = newranges;
    (*cm).numcmranges = numnewranges;
}

/*
 * subcoloronerange - do subcolorcvec's work for a high range
 */
pub unsafe fn subcoloronerange(
    v: *mut vars,
    from_in: chr,
    to: chr,
    lp: *mut state,
    rp: *mut state,
    lastsubcolor: *mut color,
) {
    let cm: *mut colormap = (*v).cm;
    let newranges: *mut colormaprange;
    let mut numnewranges: c_int;
    let mut oldrange: *mut colormaprange;
    let mut oldrangen: c_int;
    let mut newrow: c_int;
    let mut from: chr = from_in;

    /* Caller should take care of non-high-range cases */
    assert!(from > MAX_SIMPLE_CHR);
    assert!(from < to);

    /*
     * Potentially, if we have N non-adjacent ranges, we could need as many as
     * 2N+1 result ranges (consider case where new range spans 'em all).
     */
    newranges = MALLOC(
        ((*cm).numcmranges * 2 + 1) as usize * std::mem::size_of::<colormaprange>(),
    ) as *mut colormaprange;
    if newranges.is_null() {
        CERR(cm, REG_ESPACE);
        return;
    }
    numnewranges = 0;

    /* Ranges before target are unchanged */
    oldrange = (*cm).cmranges;
    oldrangen = 0;
    while oldrangen < (*cm).numcmranges {
        if (*oldrange).cmax >= from {
            break;
        }
        *newranges.add(numnewranges as usize) = *oldrange;
        numnewranges += 1;
        oldrange = oldrange.add(1);
        oldrangen += 1;
    }

    /*
     * Deal with ranges that (partially) overlap the target.  As we process
     * each such range, increase "from" to remove the dealt-with characters
     * from the target range.
     */
    while oldrangen < (*cm).numcmranges && (*oldrange).cmin <= to {
        if from < (*oldrange).cmin {
            /* Handle portion of new range that corresponds to no old range */
            (*newranges.add(numnewranges as usize)).cmin = from;
            (*newranges.add(numnewranges as usize)).cmax = (*oldrange).cmin - 1;
            /* row state should be cloned from the "all others" row */
            newrow = newhicolorrow(cm, 0);
            (*newranges.add(numnewranges as usize)).rownum = newrow;
            numnewranges += 1;
            /* Update colors in newrow and create arcs as needed */
            subcoloronerow(v, newrow, lp, rp, lastsubcolor);
            /* We've now fully processed the part of new range before old */
            from = (*oldrange).cmin;
        }

        if from <= (*oldrange).cmin && to >= (*oldrange).cmax {
            /* old range is fully contained in new, process it in-place */
            *newranges.add(numnewranges as usize) = *oldrange;
            numnewranges += 1;
            newrow = (*oldrange).rownum;
            from = (*oldrange).cmax + 1;
        } else {
            /* some part of old range does not overlap new range */
            if from > (*oldrange).cmin {
                /* emit portion of old range before new range */
                (*newranges.add(numnewranges as usize)).cmin = (*oldrange).cmin;
                (*newranges.add(numnewranges as usize)).cmax = from - 1;
                (*newranges.add(numnewranges as usize)).rownum = (*oldrange).rownum;
                numnewranges += 1;
            }
            /* emit common subrange, initially cloning from old range */
            (*newranges.add(numnewranges as usize)).cmin = from;
            (*newranges.add(numnewranges as usize)).cmax =
                if to < (*oldrange).cmax { to } else { (*oldrange).cmax };
            newrow = newhicolorrow(cm, (*oldrange).rownum);
            (*newranges.add(numnewranges as usize)).rownum = newrow;
            numnewranges += 1;
            if to < (*oldrange).cmax {
                /* emit portion of old range after new range */
                (*newranges.add(numnewranges as usize)).cmin = to + 1;
                (*newranges.add(numnewranges as usize)).cmax = (*oldrange).cmax;
                /* must clone the row if we are making two new ranges from old */
                (*newranges.add(numnewranges as usize)).rownum = if from > (*oldrange).cmin {
                    newhicolorrow(cm, (*oldrange).rownum)
                } else {
                    (*oldrange).rownum
                };
                numnewranges += 1;
            }
            from = (*oldrange).cmax + 1;
        }
        /* Update colors in newrow and create arcs as needed */
        subcoloronerow(v, newrow, lp, rp, lastsubcolor);
        /* we've now fully processed this old range */
        oldrange = oldrange.add(1);
        oldrangen += 1;
    }

    if from <= to {
        /* Handle portion of new range that corresponds to no old range */
        (*newranges.add(numnewranges as usize)).cmin = from;
        (*newranges.add(numnewranges as usize)).cmax = to;
        /* row state should be cloned from the "all others" row */
        newrow = newhicolorrow(cm, 0);
        (*newranges.add(numnewranges as usize)).rownum = newrow;
        numnewranges += 1;
        /* Update colors in newrow and create arcs as needed */
        subcoloronerow(v, newrow, lp, rp, lastsubcolor);
    }

    /* Ranges after target are unchanged */
    while oldrangen < (*cm).numcmranges {
        *newranges.add(numnewranges as usize) = *oldrange;
        numnewranges += 1;
        oldrange = oldrange.add(1);
        oldrangen += 1;
    }

    /* Assert our original space estimate was adequate */
    assert!(numnewranges <= ((*cm).numcmranges * 2 + 1));

    /* And finally, store back the updated list of ranges */
    if !(*cm).cmranges.is_null() {
        FREE((*cm).cmranges as *mut c_void);
    }
    (*cm).cmranges = newranges;
    (*cm).numcmranges = numnewranges;
}

/*
 * subcoloronerow - do subcolorcvec's work for one new row in the high colormap
 */
pub unsafe fn subcoloronerow(
    v: *mut vars,
    rownum: c_int,
    lp: *mut state,
    rp: *mut state,
    lastsubcolor: *mut color,
) {
    let cm: *mut colormap = (*v).cm;
    let mut pco: *mut color;
    let mut i: c_int;

    /* Apply subcolorhi() and make arc for each entry in row */
    pco = (*cm).hicolormap.add((rownum * (*cm).hiarraycols) as usize);
    i = 0;
    while i < (*cm).hiarraycols {
        let sco: color = subcolorhi(cm, pco);

        NOERR!(v);
        /* make the arc if needed */
        if sco != *lastsubcolor {
            newarc((*v).nfa, PLAIN, sco, lp, rp);
            NOERR!(v);
            *lastsubcolor = sco;
        }
        pco = pco.add(1);
        i += 1;
    }
}

/*
 * okcolors - promote subcolors to full colors
 */
pub unsafe fn okcolors(nfa: *mut nfa, cm: *mut colormap) {
    let mut cd: *mut colordesc;
    let end: *mut colordesc = CDEND(cm);
    let mut a: *mut arc;
    let mut co: color;

    cd = (*cm).cd;
    co = 0;
    while cd < end {
        let sco = (*cd).sub;
        if UNUSEDCOLOR(cd) != 0 || sco == NOSUB {
            /* has no subcolor, no further action */
        } else if sco == co {
            /* is subcolor, let parent deal with it */
        } else if (*cd).nschrs == 0 && (*cd).nuchrs == 0 {
            /*
             * Parent is now empty, so just change all its arcs to the
             * subcolor, then free the parent.
             *
             * It is not obvious that simply relabeling the arcs like this is
             * OK; it appears to risk creating duplicate arcs.  We are
             * basically relying on the assumption that processing of a
             * bracket expression can't create arcs of both a color and its
             * subcolor between the bracket's endpoints.
             */
            (*cd).sub = NOSUB;
            let scd: *mut colordesc = (*cm).cd.add(sco as usize);
            assert!((*scd).nschrs > 0 || (*scd).nuchrs > 0);
            assert!((*scd).sub == sco);
            (*scd).sub = NOSUB;
            loop {
                a = (*cd).arcs;
                if a.is_null() {
                    break;
                }
                assert!((*a).co == co);
                uncolorchain(cm, a);
                (*a).co = sco;
                colorchain(cm, a);
            }
            freecolor(cm, co);
        } else {
            /* parent's arcs must gain parallel subcolor arcs */
            (*cd).sub = NOSUB;
            let scd: *mut colordesc = (*cm).cd.add(sco as usize);
            assert!((*scd).nschrs > 0 || (*scd).nuchrs > 0);
            assert!((*scd).sub == sco);
            (*scd).sub = NOSUB;
            a = (*cd).arcs;
            while !a.is_null() {
                assert!((*a).co == co);
                newarc(nfa, (*a).r#type, sco, (*a).from, (*a).to);
                a = (*a).colorchain;
            }
        }
        cd = cd.add(1);
        co += 1;
    }
}

/*
 * colorchain - add this arc to the color chain of its color
 */
pub unsafe fn colorchain(cm: *mut colormap, a: *mut arc) {
    let cd: *mut colordesc = (*cm).cd.add((*a).co as usize);

    assert!((*a).co >= 0);
    if !(*cd).arcs.is_null() {
        (*(*cd).arcs).colorchainRev = a;
    }
    (*a).colorchain = (*cd).arcs;
    (*a).colorchainRev = std::ptr::null_mut();
    (*cd).arcs = a;
}

/*
 * uncolorchain - delete this arc from the color chain of its color
 */
pub unsafe fn uncolorchain(cm: *mut colormap, a: *mut arc) {
    let cd: *mut colordesc = (*cm).cd.add((*a).co as usize);
    let aa: *mut arc = (*a).colorchainRev;

    assert!((*a).co >= 0);
    if aa.is_null() {
        assert!((*cd).arcs == a);
        (*cd).arcs = (*a).colorchain;
    } else {
        assert!((*aa).colorchain == a);
        (*aa).colorchain = (*a).colorchain;
    }
    if !(*a).colorchain.is_null() {
        (*(*a).colorchain).colorchainRev = aa;
    }
    (*a).colorchain = std::ptr::null_mut(); /* paranoia */
    (*a).colorchainRev = std::ptr::null_mut();
}

/*
 * rainbow - add arcs of all full colors (but one) between specified states
 *
 * If there isn't an exception color, we now generate just a single arc
 * labeled RAINBOW, saving lots of arc-munging later on.
 */
pub unsafe fn rainbow(
    nfa: *mut nfa,
    cm: *mut colormap,
    r#type: c_int,
    but: color, /* COLORLESS if no exceptions */
    from: *mut state,
    to: *mut state,
) {
    let mut cd: *mut colordesc;
    let end: *mut colordesc = CDEND(cm);
    let mut co: color;

    if but == COLORLESS {
        newarc(nfa, r#type, RAINBOW, from, to);
        return;
    }

    /* Gotta do it the hard way.  Skip subcolors, pseudocolors, and "but" */
    cd = (*cm).cd;
    co = 0;
    while cd < end && !CISERR(cm) {
        if UNUSEDCOLOR(cd) == 0
            && (*cd).sub != co
            && co != but
            && ((*cd).flags & PSEUDO) == 0
        {
            newarc(nfa, r#type, co, from, to);
        }
        cd = cd.add(1);
        co += 1;
    }
}

/*
 * colorcomplement - add arcs of complementary colors
 *
 * We add arcs of all colors that are not pseudocolors and do not match
 * any of the "of" state's PLAIN outarcs.
 *
 * The calling sequence ought to be reconciled with cloneouts().
 */
pub unsafe fn colorcomplement(
    nfa: *mut nfa,
    cm: *mut colormap,
    r#type: c_int,
    of: *mut state,
    from: *mut state,
    to: *mut state,
) {
    let mut cd: *mut colordesc;
    let end: *mut colordesc = CDEND(cm);
    let mut co: color;
    let mut a: *mut arc;

    assert!(of != from);

    /*
     * A RAINBOW arc matches all colors, making the complement empty.  But we
     * can't just return without making any arcs, because that would leave the
     * NFA disconnected which would break any future delsub().  Instead, make
     * a CANTMATCH arc.  Also set the HASCANTMATCH flag so we know we need to
     * clean that up at the start of NFA optimization.
     */
    if !findarc(of, PLAIN, RAINBOW).is_null() {
        newarc(nfa, CANTMATCH, 0, from, to);
        (*nfa).flags |= crate::regex::regguts::HASCANTMATCH;
        return;
    }

    /* Otherwise, transiently mark the colors that appear in of's out-arcs */
    a = (*of).outs;
    while !a.is_null() {
        if (*a).r#type == PLAIN {
            assert!((*a).co >= 0);
            cd = (*cm).cd.add((*a).co as usize);
            assert!(UNUSEDCOLOR(cd) == 0);
            (*cd).flags |= COLMARK;
        }

        /*
         * There's no syntax for re-complementing a color set, so we cannot
         * see CANTMATCH arcs here.
         */
        assert!((*a).r#type != CANTMATCH);
        a = (*a).outchain;
    }

    /* Scan colors, clear transient marks, add arcs for unmarked colors */
    cd = (*cm).cd;
    co = 0;
    while cd < end && !CISERR(cm) {
        if (*cd).flags & COLMARK != 0 {
            (*cd).flags &= !COLMARK;
        } else if UNUSEDCOLOR(cd) == 0 && ((*cd).flags & PSEUDO) == 0 {
            newarc(nfa, r#type, co, from, to);
        }
        cd = cd.add(1);
        co += 1;
    }
}

// #ifdef REG_DEBUG
// dumpcolors() and dumpchr() are only compiled when REG_DEBUG is defined,
// which this port never does.  They are translated as no-op bodies to match
// the sibling REG_DEBUG dump functions in regc_nfa.rs and regcomp.rs.

/*
 * dumpcolors - debugging output
 */
unsafe fn dumpcolors(cm: *mut colormap, f: *mut c_void /* FILE *; debug output */) {
    let _ = (cm, f);
    // #ifdef REG_DEBUG -- body compiled out when REG_DEBUG is undefined.
}

/*
 * dumpchr - print a chr
 *
 * Kind of char-centric but works well enough for debug use.
 */
unsafe fn dumpchr(c: chr, f: *mut c_void /* FILE *; debug output */) {
    let _ = (c, f);
    // #ifdef REG_DEBUG -- body compiled out when REG_DEBUG is undefined.
}
// #endif /* REG_DEBUG */
