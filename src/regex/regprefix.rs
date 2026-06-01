//! regex/regprefix.c - Extract a common prefix, if any, from a compiled regex.

use crate::prelude::*;

use crate::regex::regcustom::chr;
use crate::regex::regerror::{
    REG_ESPACE, REG_EXACT, REG_INVARG, REG_MIXED, REG_NOMATCH, REG_PREFIX,
};
use crate::regex::regex::{regex_t, REG_UIMPOSSIBLE};
use crate::regex::regguts::{
    carc, cnfa, color, colormap, guts, pg_set_regex_collation, GETCOLOR, COLORLESS, MATCHALL,
    RAINBOW, REMAGIC,
};
use crate::utils::palloc::MCXT_ALLOC_NO_OOM;

use std::ffi::c_int;
use std::ffi::c_void;

/// C: #define MALLOC(n) palloc_extended((n), MCXT_ALLOC_NO_OOM)
/// Returns NULL on failure rather than throwing.
unsafe fn MALLOC(n: usize) -> *mut c_void {
    palloc_extended(n, MCXT_ALLOC_NO_OOM)
}

/// C: #define FREE(p) pfree(VS(p))
unsafe fn FREE(p: *mut c_void) {
    pfree(p);
}

/*
 * pg_regprefix - get common prefix for regular expression
 *
 * Returns one of:
 *	REG_NOMATCH: there is no common prefix of strings matching the regex
 *	REG_PREFIX: there is a common prefix of strings matching the regex
 *	REG_EXACT: all strings satisfying the regex must match the same string
 *	or a REG_XXX error code
 *
 * In the non-failure cases, *string is set to a palloc'd string containing
 * the common prefix or exact value, of length *slength (measured in chrs
 * not bytes!).
 *
 * This function does not analyze all complex cases (such as lookaround
 * constraints) exactly.  Therefore it is possible that some strings matching
 * the reported prefix or exact-match string do not satisfy the regex.  But
 * it should never be the case that a string satisfying the regex does not
 * match the reported prefix or exact-match string.
 */
pub unsafe fn pg_regprefix(
    re: *mut regex_t,
    string: *mut *mut chr,
    slength: *mut usize,
) -> c_int {
    let g: *mut guts;
    let cnfa: *mut cnfa;
    let st: c_int;

    /* sanity checks */
    if string.is_null() || slength.is_null() {
        return REG_INVARG;
    }
    *string = std::ptr::null_mut(); /* initialize for failure cases */
    *slength = 0;
    if re.is_null() || (*re).re_magic != REMAGIC {
        return REG_INVARG;
    }
    if (*re).re_csize != std::mem::size_of::<chr>() as c_int {
        return REG_MIXED;
    }

    /* Initialize locale-dependent support */
    pg_set_regex_collation((*re).re_collation);

    /* setup */
    g = (*re).re_guts as *mut guts;
    if (*g).info & REG_UIMPOSSIBLE != 0 {
        return REG_NOMATCH;
    }

    /*
     * This implementation considers only the search NFA for the topmost regex
     * tree node.  Therefore, constraints such as backrefs are not fully
     * applied, which is allowed per the function's API spec.
     */
    Assert!(!(*g).tree.is_null());
    cnfa = &mut (*(*g).tree).cnfa;

    /* matchall NFAs never have a fixed prefix */
    if (*cnfa).flags & MATCHALL != 0 {
        return REG_NOMATCH;
    }

    /*
     * Since a correct NFA should never contain any exit-free loops, it should
     * not be possible for our traversal to return to a previously visited NFA
     * state.  Hence we need at most nstates chrs in the output string.
     */
    *string = MALLOC((*cnfa).nstates as usize * std::mem::size_of::<chr>()) as *mut chr;
    if (*string).is_null() {
        return REG_ESPACE;
    }

    /* do it */
    st = findprefix(cnfa, &mut (*g).cmap, *string, slength);

    Assert!(*slength <= (*cnfa).nstates as usize);

    /* clean up */
    if st != REG_PREFIX && st != REG_EXACT {
        FREE(*string as *mut c_void);
        *string = std::ptr::null_mut();
        *slength = 0;
    }

    st
}

/*
 * findprefix - extract common prefix from cNFA
 *
 * Results are returned into the preallocated chr array string[], with
 * *slength (which must be preset to zero) incremented for each chr.
 */
unsafe fn findprefix(
    cnfa: *mut cnfa,
    cm: *mut colormap,
    string: *mut chr,
    slength: *mut usize,
) -> c_int {
    let mut st: c_int;
    let mut nextst: c_int;
    let mut thiscolor: color;
    let mut c: chr;
    let mut ca: *mut carc;

    /*
     * The "pre" state must have only BOS/BOL outarcs, else pattern isn't
     * anchored left.  If we have both BOS and BOL, they must go to the same
     * next state.
     */
    st = (*cnfa).pre;
    nextst = -1;
    ca = *(*cnfa).states.offset(st as isize);
    while (*ca).co != COLORLESS {
        if (*ca).co == (*cnfa).bos[0] || (*ca).co == (*cnfa).bos[1] {
            if nextst == -1 {
                nextst = (*ca).to;
            } else if nextst != (*ca).to {
                return REG_NOMATCH;
            }
        } else {
            return REG_NOMATCH;
        }
        ca = ca.offset(1);
    }
    if nextst == -1 {
        return REG_NOMATCH;
    }

    /*
     * Scan through successive states, stopping as soon as we find one with
     * more than one acceptable transition character (either multiple colors
     * on out-arcs, or a color with more than one member chr).
     *
     * We could find a state with multiple out-arcs that are all labeled with
     * the same singleton color; this comes from patterns like "^ab(cde|cxy)".
     * In that case we add the chr "c" to the output string but then exit the
     * loop with nextst == -1.  This leaves a little bit on the table: if the
     * pattern is like "^ab(cde|cdy)", we won't notice that "d" could be added
     * to the prefix.  But chasing multiple parallel state chains doesn't seem
     * worth the trouble.
     */
    loop {
        st = nextst;
        nextst = -1;
        thiscolor = COLORLESS;
        ca = *(*cnfa).states.offset(st as isize);
        while (*ca).co != COLORLESS {
            /* We can ignore BOS/BOL arcs */
            if (*ca).co == (*cnfa).bos[0] || (*ca).co == (*cnfa).bos[1] {
                ca = ca.offset(1);
                continue;
            }

            /*
             * ... but EOS/EOL arcs terminate the search, as do RAINBOW arcs
             * and LACONs
             */
            if (*ca).co == (*cnfa).eos[0]
                || (*ca).co == (*cnfa).eos[1]
                || (*ca).co == RAINBOW
                || (*ca).co as c_int >= (*cnfa).ncolors
            {
                thiscolor = COLORLESS;
                break;
            }
            if thiscolor == COLORLESS {
                /* First plain outarc */
                thiscolor = (*ca).co;
                nextst = (*ca).to;
            } else if thiscolor == (*ca).co {
                /* Another plain outarc for same color */
                nextst = -1;
            } else {
                /* More than one plain outarc color terminates the search */
                thiscolor = COLORLESS;
                break;
            }
            ca = ca.offset(1);
        }
        /* Done if we didn't find exactly one color on plain outarcs */
        if thiscolor == COLORLESS {
            break;
        }
        /* The color must be a singleton */
        if (*(*cm).cd.offset(thiscolor as isize)).nschrs != 1 {
            break;
        }
        /* Must not have any high-color-map entries */
        if (*(*cm).cd.offset(thiscolor as isize)).nuchrs != 0 {
            break;
        }

        /*
         * Identify the color's sole member chr and add it to the prefix
         * string.  In general the colormap data structure doesn't provide a
         * way to find color member chrs, except by trying GETCOLOR() on each
         * possible chr value, which won't do at all.  However, for the cases
         * we care about it should be sufficient to test the "firstchr" value,
         * that is the first chr ever added to the color.  There are cases
         * where this might no longer be a member of the color (so we do need
         * to test), but none of them are likely to arise for a character that
         * is a member of a common prefix.  If we do hit such a corner case,
         * we just fall out without adding anything to the prefix string.
         */
        c = (*(*cm).cd.offset(thiscolor as isize)).firstchr;
        if GETCOLOR(cm, c) != thiscolor {
            break;
        }

        *string.offset(*slength as isize) = c;
        *slength += 1;

        /* Advance to next state, but only if we have a unique next state */
        if nextst == -1 {
            break;
        }
    }

    /*
     * If we ended at a state that only has EOS/EOL outarcs leading to the
     * "post" state, then we have an exact-match string.  Note this is true
     * even if the string is of zero length.
     */
    nextst = -1;
    ca = *(*cnfa).states.offset(st as isize);
    while (*ca).co != COLORLESS {
        if (*ca).co == (*cnfa).eos[0] || (*ca).co == (*cnfa).eos[1] {
            if nextst == -1 {
                nextst = (*ca).to;
            } else if nextst != (*ca).to {
                nextst = -1;
                break;
            }
        } else {
            nextst = -1;
            break;
        }
        ca = ca.offset(1);
    }
    if nextst == (*cnfa).post {
        return REG_EXACT;
    }

    /*
     * Otherwise, if we were unable to identify any prefix characters, say
     * NOMATCH --- the pattern is anchored left, but doesn't specify any
     * particular first character.
     */
    if *slength > 0 {
        return REG_PREFIX;
    }

    REG_NOMATCH
}
