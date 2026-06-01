//! regex/regexport.c - Functions for exporting info about a regex's NFA
//!
//! In this implementation, the NFA defines a necessary but not sufficient
//! condition for a string to match the regex: that is, there can be strings
//! that match the NFA but don't match the full regex, but not vice versa.
//! Thus, for example, it is okay for the functions below to treat lookaround
//! constraints as no-ops, since they merely constrain the string some more.

use crate::prelude::*;

use crate::mb::pg_wchar::pg_wchar;
use crate::utils::misc::stack_depth::check_stack_depth;
use crate::regex::regcustom::{chr, CHR_MIN, MAX_SIMPLE_CHR};
use crate::regex::regex::regex_t;
use crate::regex::regguts::{
    carc, cnfa, color, colormap, guts, COLORLESS, PSEUDO, REMAGIC,
};

/* These macros must match corresponding ones in regguts.h: */
/// color for chars not appearing in regex
pub const COLOR_WHITE: c_int = 0;
/// represents all colors except pseudocolors
pub const COLOR_RAINBOW: c_int = -2;

/// information about one arc of a regex's NFA
#[repr(C)]
#[derive(Clone, Copy)]
pub struct regex_arc_t {
    /// label (character-set color) of arc
    pub co: c_int,
    /// next state number
    pub to: c_int,
}

/*
 * Get total number of NFA states.
 */
pub unsafe fn pg_reg_getnumstates(regex: *const regex_t) -> c_int {
    let cnfa: *mut cnfa;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cnfa = &mut (*((*regex).re_guts as *mut guts)).search;

    (*cnfa).nstates
}

/*
 * Get initial state of NFA.
 */
pub unsafe fn pg_reg_getinitialstate(regex: *const regex_t) -> c_int {
    let cnfa: *mut cnfa;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cnfa = &mut (*((*regex).re_guts as *mut guts)).search;

    (*cnfa).pre
}

/*
 * Get final state of NFA.
 */
pub unsafe fn pg_reg_getfinalstate(regex: *const regex_t) -> c_int {
    let cnfa: *mut cnfa;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cnfa = &mut (*((*regex).re_guts as *mut guts)).search;

    (*cnfa).post
}

/*
 * pg_reg_getnumoutarcs() and pg_reg_getoutarcs() mask the existence of LACON
 * arcs from the caller, treating any LACON as being automatically satisfied.
 * Since the output representation does not support arcs that consume no
 * character when traversed, we have to recursively traverse LACON arcs here,
 * and report whatever normal arcs are reachable by traversing LACON arcs.
 * Note that this wouldn't work if it were possible to reach the final state
 * via LACON traversal, but the regex library never builds NFAs that have
 * LACON arcs leading directly to the final state.  (This is because the
 * regex executor is designed to consume one character beyond the nominal
 * match end --- possibly an EOS indicator --- so there is always a set of
 * ordinary arcs leading to the final state.)
 *
 * traverse_lacons is a recursive subroutine used by both exported functions
 * to count and then emit the reachable regular arcs.  *arcs_count is
 * incremented by the number of reachable arcs, and as many as will fit in
 * arcs_len (possibly 0) are emitted into arcs[].
 */
unsafe fn traverse_lacons(
    cnfa: *mut cnfa,
    st: c_int,
    arcs_count: *mut c_int,
    arcs: *mut regex_arc_t,
    arcs_len: c_int,
) {
    let mut ca: *mut carc;

    /*
     * Since this function recurses, it could theoretically be driven to stack
     * overflow.  In practice, this is mostly useful to backstop against a
     * failure of the regex compiler to remove a loop of LACON arcs.
     */
    check_stack_depth();

    ca = *(*cnfa).states.add(st as usize);
    while (*ca).co != COLORLESS {
        if ((*ca).co as c_int) < (*cnfa).ncolors {
            /* Ordinary arc, so count and possibly emit it */
            let ndx: c_int = *arcs_count;
            *arcs_count += 1;

            if ndx < arcs_len {
                (*arcs.add(ndx as usize)).co = (*ca).co as c_int;
                (*arcs.add(ndx as usize)).to = (*ca).to;
            }
        } else {
            /* LACON arc --- assume it's satisfied and recurse... */
            /* ... but first, assert it doesn't lead directly to post state */
            Assert!((*ca).to != (*cnfa).post);

            traverse_lacons(cnfa, (*ca).to, arcs_count, arcs, arcs_len);
        }
        ca = ca.add(1);
    }
}

/*
 * Get number of outgoing NFA arcs of state number "st".
 */
pub unsafe fn pg_reg_getnumoutarcs(regex: *const regex_t, st: c_int) -> c_int {
    let cnfa: *mut cnfa;
    let mut arcs_count: c_int;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cnfa = &mut (*((*regex).re_guts as *mut guts)).search;

    if st < 0 || st >= (*cnfa).nstates {
        return 0;
    }
    arcs_count = 0;
    traverse_lacons(cnfa, st, &mut arcs_count, null_mut(), 0);
    arcs_count
}

/*
 * Write array of outgoing NFA arcs of state number "st" into arcs[],
 * whose length arcs_len must be at least as long as indicated by
 * pg_reg_getnumoutarcs(), else not all arcs will be returned.
 */
pub unsafe fn pg_reg_getoutarcs(
    regex: *const regex_t,
    st: c_int,
    arcs: *mut regex_arc_t,
    arcs_len: c_int,
) {
    let cnfa: *mut cnfa;
    let mut arcs_count: c_int;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cnfa = &mut (*((*regex).re_guts as *mut guts)).search;

    if st < 0 || st >= (*cnfa).nstates || arcs_len <= 0 {
        return;
    }
    arcs_count = 0;
    traverse_lacons(cnfa, st, &mut arcs_count, arcs, arcs_len);
}

/*
 * Get total number of colors.
 */
pub unsafe fn pg_reg_getnumcolors(regex: *const regex_t) -> c_int {
    let cm: *mut colormap;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cm = &mut (*((*regex).re_guts as *mut guts)).cmap;

    (*cm).max as c_int + 1
}

/*
 * Check if color is beginning of line/string.
 *
 * (We might at some point need to offer more refined handling of pseudocolors,
 * but this will do for now.)
 */
pub unsafe fn pg_reg_colorisbegin(regex: *const regex_t, co: c_int) -> c_int {
    let cnfa: *mut cnfa;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cnfa = &mut (*((*regex).re_guts as *mut guts)).search;

    if co == (*cnfa).bos[0] as c_int || co == (*cnfa).bos[1] as c_int {
        true as c_int
    } else {
        false as c_int
    }
}

/*
 * Check if color is end of line/string.
 */
pub unsafe fn pg_reg_colorisend(regex: *const regex_t, co: c_int) -> c_int {
    let cnfa: *mut cnfa;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cnfa = &mut (*((*regex).re_guts as *mut guts)).search;

    if co == (*cnfa).eos[0] as c_int || co == (*cnfa).eos[1] as c_int {
        true as c_int
    } else {
        false as c_int
    }
}

/*
 * Get number of member chrs of color number "co".
 *
 * Note: we return -1 if the color number is invalid, or if it is a special
 * color (WHITE, RAINBOW, or a pseudocolor), or if the number of members is
 * uncertain.
 * Callers should not try to extract the members if -1 is returned.
 */
pub unsafe fn pg_reg_getnumcharacters(regex: *const regex_t, co: c_int) -> c_int {
    let cm: *mut colormap;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cm = &mut (*((*regex).re_guts as *mut guts)).cmap;

    if co <= 0 || co > (*cm).max as c_int {
        /* <= 0 rejects WHITE and RAINBOW */
        return -1;
    }
    if (*(*cm).cd.add(co as usize)).flags & PSEUDO != 0 {
        /* also pseudocolors (BOS etc) */
        return -1;
    }

    /*
     * If the color appears anywhere in the high colormap, treat its number of
     * members as uncertain.  In principle we could determine all the specific
     * chrs corresponding to each such entry, but it would be expensive
     * (particularly if character class tests are required) and it doesn't
     * seem worth it.
     */
    if (*(*cm).cd.add(co as usize)).nuchrs != 0 {
        return -1;
    }

    /* OK, return the known number of member chrs */
    (*(*cm).cd.add(co as usize)).nschrs
}

/*
 * Write array of member chrs of color number "co" into chars[],
 * whose length chars_len must be at least as long as indicated by
 * pg_reg_getnumcharacters(), else not all chars will be returned.
 *
 * Fetching the members of WHITE, RAINBOW, or a pseudocolor is not supported.
 *
 * Caution: this is a relatively expensive operation.
 */
pub unsafe fn pg_reg_getcharacters(
    regex: *const regex_t,
    co: c_int,
    chars: *mut pg_wchar,
    chars_len: c_int,
) {
    let cm: *mut colormap;
    let mut c: chr;
    let mut chars = chars;
    let mut chars_len = chars_len;

    Assert!(!regex.is_null() && (*regex).re_magic == REMAGIC);
    cm = &mut (*((*regex).re_guts as *mut guts)).cmap;

    if co <= 0 || co > (*cm).max as c_int || chars_len <= 0 {
        return;
    }
    if (*(*cm).cd.add(co as usize)).flags & PSEUDO != 0 {
        return;
    }

    /*
     * We need only examine the low character map; there should not be any
     * matching entries in the high map.
     */
    c = CHR_MIN;
    while c <= MAX_SIMPLE_CHR {
        if *(*cm).locolormap.add((c - CHR_MIN) as usize) == co as color {
            *chars = c;
            chars = chars.add(1);
            chars_len -= 1;
            if chars_len == 0 {
                break;
            }
        }
        c += 1;
    }
}
