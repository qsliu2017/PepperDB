//! Multi-column page splitting algorithm for multi-column GiST indexes.
//!
//! Faithful 1:1 translation of PostgreSQL 18.3
//! src/backend/access/gist/gistsplit.c
//!
//! This file is concerned with making good page-split decisions in multi-column
//! GiST indexes.  The opclass-specific picksplit functions can only be expected
//! to produce answers based on a single column.  We first run the picksplit
//! function for column 1; then, if there are more columns, we check if any of
//! the tuples are "don't cares" so far as the column 1 split is concerned
//! (that is, they could go to either side for no additional penalty).  If so,
//! we try to redistribute those tuples on the basis of the next column.
//! Repeat till we're out of columns.
//!
//! gistSplitByKey() is the entry point to this file.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::common::indextuple::{index_getattr, IndexTuple, INDEX_MAX_KEYS};
use crate::access::gist::gist_private::{
    gistDeCompressAtt, gistKeyIsEQ, gistMakeUnionItVec, gistMakeUnionKey, gistdentryinit,
    gistpenalty, GistSplitVector, GISTENTRY, GISTSTATE, GIST_SPLITVEC,
};
use crate::storage::bufpage::Page;
use crate::storage::off::{
    FirstOffsetNumber, InvalidOffsetNumber, OffsetNumber, OffsetNumberNext,
};
use crate::utils::fmgr::FunctionCall2Coll;
use crate::utils::rel::{Relation, RelationGetRelationName};

// ===========================================================================
// access/gist.h stubs (NOT yet ported in its own module).
// TODO: dedup once access/gist.h is ported.
// ===========================================================================

/// access/gist.h: vector of GISTENTRY with a leading count, as passed to the
/// union/picksplit opclass support functions.
#[repr(C)]
pub struct GistEntryVector {
    pub n: int32,
    pub vector: [GISTENTRY; FLEXIBLE_ARRAY_MEMBER],
}

/* #define GEVHDRSZ ((Size) offsetof(GistEntryVector, vector)) */
pub const GEVHDRSZ: Size = core::mem::offset_of!(GistEntryVector, vector) as Size;

/* #define gistentryinit(e, k, r, pg, o, l) ... -- initialize a GISTENTRY */
#[inline]
unsafe fn gistentryinit(
    e: *mut GISTENTRY,
    k: Datum,
    r: Relation,
    pg: Page,
    o: OffsetNumber,
    l: bool,
) {
    (*e).key = k;
    (*e).rel = r;
    (*e).page = pg;
    (*e).offset = o;
    (*e).leafkey = l;
}

#[repr(C)]
struct GistSplitUnion {
    entries: *mut OffsetNumber,
    len: c_int,
    attr: *mut Datum,
    isnull: *mut bool,
    dontcare: *mut bool,
}

/*
 * Form unions of subkeys in itvec[] entries listed in gsvp->entries[],
 * ignoring any tuples that are marked in gsvp->dontcare[].  Subroutine for
 * gistunionsubkey.
 */
unsafe fn gistunionsubkeyvec(
    giststate: *mut GISTSTATE,
    itvec: *mut IndexTuple,
    gsvp: *mut GistSplitUnion,
) {
    let cleanedItVec: *mut IndexTuple;
    let mut i: c_int;
    let mut cleanedLen: c_int = 0;

    cleanedItVec = palloc(core::mem::size_of::<IndexTuple>() * (*gsvp).len as usize)
        as *mut IndexTuple;

    i = 0;
    while i < (*gsvp).len {
        if !(*gsvp).dontcare.is_null()
            && *(*gsvp)
                .dontcare
                .add(*(*gsvp).entries.add(i as usize) as usize)
        {
            i += 1;
            continue;
        }

        *cleanedItVec.add(cleanedLen as usize) =
            *itvec.add((*(*gsvp).entries.add(i as usize) - 1) as usize);
        cleanedLen += 1;
        i += 1;
    }

    gistMakeUnionItVec(
        giststate,
        cleanedItVec,
        cleanedLen,
        (*gsvp).attr,
        (*gsvp).isnull,
    );

    pfree(cleanedItVec as *mut c_void);
}

/*
 * Recompute unions of left- and right-side subkeys after a page split,
 * ignoring any tuples that are marked in spl->spl_dontcare[].
 *
 * Note: we always recompute union keys for all index columns.  In some cases
 * this might represent duplicate work for the leftmost column(s), but it's
 * not safe to assume that "zero penalty to move a tuple" means "the union
 * key doesn't change at all".  Penalty functions aren't 100% accurate.
 */
unsafe fn gistunionsubkey(
    giststate: *mut GISTSTATE,
    itvec: *mut IndexTuple,
    spl: *mut GistSplitVector,
) {
    let mut gsvp: GistSplitUnion = core::mem::zeroed();

    gsvp.dontcare = (*spl).spl_dontcare;

    gsvp.entries = (*spl).splitVector.spl_left;
    gsvp.len = (*spl).splitVector.spl_nleft;
    gsvp.attr = (*spl).spl_lattr.as_mut_ptr();
    gsvp.isnull = (*spl).spl_lisnull.as_mut_ptr();

    gistunionsubkeyvec(giststate, itvec, &mut gsvp);

    gsvp.entries = (*spl).splitVector.spl_right;
    gsvp.len = (*spl).splitVector.spl_nright;
    gsvp.attr = (*spl).spl_rattr.as_mut_ptr();
    gsvp.isnull = (*spl).spl_risnull.as_mut_ptr();

    gistunionsubkeyvec(giststate, itvec, &mut gsvp);
}

/*
 * Find tuples that are "don't cares", that is could be moved to the other
 * side of the split with zero penalty, so far as the attno column is
 * concerned.
 *
 * Don't-care tuples are marked by setting the corresponding entry in
 * spl->spl_dontcare[] to "true".  Caller must have initialized that array
 * to zeroes.
 *
 * Returns number of don't-cares found.
 */
unsafe fn findDontCares(
    r: Relation,
    giststate: *mut GISTSTATE,
    valvec: *mut GISTENTRY,
    spl: *mut GistSplitVector,
    attno: c_int,
) -> c_int {
    let mut i: c_int;
    let mut entry: GISTENTRY = core::mem::zeroed();
    let mut NumDontCare: c_int = 0;

    /*
     * First, search the left-side tuples to see if any have zero penalty to
     * be added to the right-side union key.
     *
     * attno column is known all-not-null (see gistSplitByKey), so we need not
     * check for nulls
     */
    gistentryinit(
        &mut entry,
        (*spl).splitVector.spl_rdatum,
        r,
        null_mut(),
        0 as OffsetNumber,
        false,
    );
    i = 0;
    while i < (*spl).splitVector.spl_nleft {
        let j = *(*spl).splitVector.spl_left.add(i as usize) as c_int;
        let penalty = gistpenalty(
            giststate,
            attno,
            &mut entry,
            false,
            valvec.add(j as usize),
            false,
        );

        if penalty == 0.0 {
            *(*spl).spl_dontcare.add(j as usize) = true;
            NumDontCare += 1;
        }
        i += 1;
    }

    /* And conversely for the right-side tuples */
    gistentryinit(
        &mut entry,
        (*spl).splitVector.spl_ldatum,
        r,
        null_mut(),
        0 as OffsetNumber,
        false,
    );
    i = 0;
    while i < (*spl).splitVector.spl_nright {
        let j = *(*spl).splitVector.spl_right.add(i as usize) as c_int;
        let penalty = gistpenalty(
            giststate,
            attno,
            &mut entry,
            false,
            valvec.add(j as usize),
            false,
        );

        if penalty == 0.0 {
            *(*spl).spl_dontcare.add(j as usize) = true;
            NumDontCare += 1;
        }
        i += 1;
    }

    NumDontCare
}

/*
 * Remove tuples that are marked don't-cares from the tuple index array a[]
 * of length *len.  This is applied separately to the spl_left and spl_right
 * arrays.
 */
unsafe fn removeDontCares(a: *mut OffsetNumber, len: *mut c_int, dontcare: *const bool) {
    let origlen: c_int;
    let mut newlen: c_int;
    let mut i: c_int;
    let mut curwpos: *mut OffsetNumber;

    origlen = *len;
    newlen = *len;
    curwpos = a;
    i = 0;
    while i < origlen {
        let ai = *a.add(i as usize);

        if *dontcare.add(ai as usize) == false {
            /* re-emit item into a[] */
            *curwpos = ai;
            curwpos = curwpos.add(1);
        } else {
            newlen -= 1;
        }
        i += 1;
    }

    *len = newlen;
}

/*
 * Place a single don't-care tuple into either the left or right side of the
 * split, according to which has least penalty for merging the tuple into
 * the previously-computed union keys.  We need consider only columns starting
 * at attno.
 */
unsafe fn placeOne(
    r: Relation,
    giststate: *mut GISTSTATE,
    v: *mut GistSplitVector,
    itup: IndexTuple,
    off: OffsetNumber,
    mut attno: c_int,
) {
    let mut identry: [GISTENTRY; INDEX_MAX_KEYS as usize] = core::mem::zeroed();
    let mut isnull: [bool; INDEX_MAX_KEYS as usize] = [false; INDEX_MAX_KEYS as usize];
    let mut toLeft: bool = true;

    gistDeCompressAtt(
        giststate,
        r,
        itup,
        null_mut(),
        0 as OffsetNumber,
        identry.as_mut_ptr(),
        isnull.as_mut_ptr(),
    );

    while attno < (*(*giststate).nonLeafTupdesc).natts {
        let lpenalty: f32;
        let rpenalty: f32;
        let mut entry: GISTENTRY = core::mem::zeroed();

        gistentryinit(
            &mut entry,
            (*v).spl_lattr[attno as usize],
            r,
            null_mut(),
            0,
            false,
        );
        lpenalty = gistpenalty(
            giststate,
            attno,
            &mut entry,
            (*v).spl_lisnull[attno as usize],
            identry.as_mut_ptr().add(attno as usize),
            isnull[attno as usize],
        );
        gistentryinit(
            &mut entry,
            (*v).spl_rattr[attno as usize],
            r,
            null_mut(),
            0,
            false,
        );
        rpenalty = gistpenalty(
            giststate,
            attno,
            &mut entry,
            (*v).spl_risnull[attno as usize],
            identry.as_mut_ptr().add(attno as usize),
            isnull[attno as usize],
        );

        if lpenalty != rpenalty {
            if lpenalty > rpenalty {
                toLeft = false;
            }
            break;
        }
        attno += 1;
    }

    if toLeft {
        *(*v)
            .splitVector
            .spl_left
            .add((*v).splitVector.spl_nleft as usize) = off;
        (*v).splitVector.spl_nleft += 1;
    } else {
        *(*v)
            .splitVector
            .spl_right
            .add((*v).splitVector.spl_nright as usize) = off;
        (*v).splitVector.spl_nright += 1;
    }
}

/*
 * Clean up when we did a secondary split but the user-defined PickSplit
 * method didn't support it (leaving spl_ldatum_exists or spl_rdatum_exists
 * true).
 *
 * We consider whether to swap the left and right outputs of the secondary
 * split; this can be worthwhile if the penalty for merging those tuples into
 * the previously chosen sets is less that way.
 *
 * In any case we must update the union datums for the current column by
 * adding in the previous union keys (oldL/oldR), since the user-defined
 * PickSplit method didn't do so.
 */
unsafe fn supportSecondarySplit(
    r: Relation,
    giststate: *mut GISTSTATE,
    attno: c_int,
    sv: *mut GIST_SPLITVEC,
    oldL: Datum,
    oldR: Datum,
) {
    let mut leaveOnLeft: bool = true;
    let mut tmpBool: bool = false;
    let mut entryL: GISTENTRY = core::mem::zeroed();
    let mut entryR: GISTENTRY = core::mem::zeroed();
    let mut entrySL: GISTENTRY = core::mem::zeroed();
    let mut entrySR: GISTENTRY = core::mem::zeroed();

    gistentryinit(&mut entryL, oldL, r, null_mut(), 0, false);
    gistentryinit(&mut entryR, oldR, r, null_mut(), 0, false);
    gistentryinit(&mut entrySL, (*sv).spl_ldatum, r, null_mut(), 0, false);
    gistentryinit(&mut entrySR, (*sv).spl_rdatum, r, null_mut(), 0, false);

    if (*sv).spl_ldatum_exists && (*sv).spl_rdatum_exists {
        let penalty1: f32;
        let penalty2: f32;

        penalty1 = gistpenalty(giststate, attno, &mut entryL, false, &mut entrySL, false)
            + gistpenalty(giststate, attno, &mut entryR, false, &mut entrySR, false);
        penalty2 = gistpenalty(giststate, attno, &mut entryL, false, &mut entrySR, false)
            + gistpenalty(giststate, attno, &mut entryR, false, &mut entrySL, false);

        if penalty1 > penalty2 {
            leaveOnLeft = false;
        }
    } else {
        let entry1: *mut GISTENTRY = if (*sv).spl_ldatum_exists {
            &mut entryL
        } else {
            &mut entryR
        };
        let penalty1: f32;
        let penalty2: f32;

        /*
         * There is only one previously defined union, so we just choose swap
         * or not by lowest penalty for that side.  We can only get here if a
         * secondary split happened to have all NULLs in its column in the
         * tuples that the outer recursion level had assigned to one side.
         * (Note that the null checks in gistSplitByKey don't prevent the
         * case, because they'll only be checking tuples that were considered
         * don't-cares at the outer recursion level, not the tuples that went
         * into determining the passed-down left and right union keys.)
         */
        penalty1 = gistpenalty(giststate, attno, entry1, false, &mut entrySL, false);
        penalty2 = gistpenalty(giststate, attno, entry1, false, &mut entrySR, false);

        if penalty1 < penalty2 {
            leaveOnLeft = (*sv).spl_ldatum_exists;
        } else {
            leaveOnLeft = (*sv).spl_rdatum_exists;
        }
    }

    if leaveOnLeft == false {
        /*
         * swap left and right
         */
        let off: *mut OffsetNumber;
        let noff: c_int;
        let datum: Datum;

        /* SWAPVAR(sv->spl_left, sv->spl_right, off) */
        off = (*sv).spl_left;
        (*sv).spl_left = (*sv).spl_right;
        (*sv).spl_right = off;
        /* SWAPVAR(sv->spl_nleft, sv->spl_nright, noff) */
        noff = (*sv).spl_nleft;
        (*sv).spl_nleft = (*sv).spl_nright;
        (*sv).spl_nright = noff;
        /* SWAPVAR(sv->spl_ldatum, sv->spl_rdatum, datum) */
        datum = (*sv).spl_ldatum;
        (*sv).spl_ldatum = (*sv).spl_rdatum;
        (*sv).spl_rdatum = datum;
        gistentryinit(&mut entrySL, (*sv).spl_ldatum, r, null_mut(), 0, false);
        gistentryinit(&mut entrySR, (*sv).spl_rdatum, r, null_mut(), 0, false);
    }

    if (*sv).spl_ldatum_exists {
        gistMakeUnionKey(
            giststate,
            attno,
            &mut entryL,
            false,
            &mut entrySL,
            false,
            &mut (*sv).spl_ldatum,
            &mut tmpBool,
        );
    }

    if (*sv).spl_rdatum_exists {
        gistMakeUnionKey(
            giststate,
            attno,
            &mut entryR,
            false,
            &mut entrySR,
            false,
            &mut (*sv).spl_rdatum,
            &mut tmpBool,
        );
    }

    (*sv).spl_ldatum_exists = false;
    (*sv).spl_rdatum_exists = false;
}

/*
 * Trivial picksplit implementation. Function called only
 * if user-defined picksplit puts all keys on the same side of the split.
 * That is a bug of user-defined picksplit but we don't want to fail.
 */
unsafe fn genericPickSplit(
    giststate: *mut GISTSTATE,
    entryvec: *mut GistEntryVector,
    v: *mut GIST_SPLITVEC,
    attno: c_int,
) {
    let mut i: OffsetNumber;
    let maxoff: OffsetNumber;
    let nbytes: c_int;
    let evec: *mut GistEntryVector;

    maxoff = ((*entryvec).n - 1) as OffsetNumber;

    nbytes = (maxoff as c_int + 2) * core::mem::size_of::<OffsetNumber>() as c_int;

    (*v).spl_left = palloc(nbytes as usize) as *mut OffsetNumber;
    (*v).spl_right = palloc(nbytes as usize) as *mut OffsetNumber;
    (*v).spl_nleft = 0;
    (*v).spl_nright = 0;

    i = FirstOffsetNumber;
    while i <= maxoff {
        if i as c_int <= (maxoff as c_int - FirstOffsetNumber as c_int + 1) / 2 {
            *(*v).spl_left.add((*v).spl_nleft as usize) = i;
            (*v).spl_nleft += 1;
        } else {
            *(*v).spl_right.add((*v).spl_nright as usize) = i;
            (*v).spl_nright += 1;
        }
        i = OffsetNumberNext(i);
    }

    /*
     * Form union datums for each side
     */
    evec = palloc(
        core::mem::size_of::<GISTENTRY>() * (*entryvec).n as usize + GEVHDRSZ as usize,
    ) as *mut GistEntryVector;

    (*evec).n = (*v).spl_nleft;
    core::ptr::copy_nonoverlapping(
        (*entryvec).vector.as_ptr().add(FirstOffsetNumber as usize),
        (*evec).vector.as_mut_ptr(),
        (*evec).n as usize,
    );
    (*v).spl_ldatum = FunctionCall2Coll(
        &mut (*giststate).unionFn[attno as usize],
        (*giststate).supportCollation[attno as usize],
        PointerGetDatum(evec as *const c_void),
        PointerGetDatum(&nbytes as *const c_int as *const c_void),
    );

    (*evec).n = (*v).spl_nright;
    core::ptr::copy_nonoverlapping(
        (*entryvec)
            .vector
            .as_ptr()
            .add(FirstOffsetNumber as usize + (*v).spl_nleft as usize),
        (*evec).vector.as_mut_ptr(),
        (*evec).n as usize,
    );
    (*v).spl_rdatum = FunctionCall2Coll(
        &mut (*giststate).unionFn[attno as usize],
        (*giststate).supportCollation[attno as usize],
        PointerGetDatum(evec as *const c_void),
        PointerGetDatum(&nbytes as *const c_int as *const c_void),
    );
}

/*
 * Calls user picksplit method for attno column to split tuples into
 * two vectors.
 *
 * Returns false if split is complete (there are no more index columns, or
 * there is no need to consider them because split is optimal already).
 *
 * Returns true and v->spl_dontcare = NULL if the picksplit result is
 * degenerate (all tuples seem to be don't-cares), so we should just
 * disregard this column and split on the next column(s) instead.
 *
 * Returns true and v->spl_dontcare != NULL if there are don't-care tuples
 * that could be relocated based on the next column(s).  The don't-care
 * tuples have been removed from the split and must be reinserted by caller.
 * There is at least one non-don't-care tuple on each side of the split,
 * and union keys for all columns are updated to include just those tuples.
 *
 * A true result implies there is at least one more index column.
 */
unsafe fn gistUserPicksplit(
    r: Relation,
    entryvec: *mut GistEntryVector,
    attno: c_int,
    v: *mut GistSplitVector,
    itup: *mut IndexTuple,
    len: c_int,
    giststate: *mut GISTSTATE,
) -> bool {
    let sv: *mut GIST_SPLITVEC = &mut (*v).splitVector;

    /*
     * Prepare spl_ldatum/spl_rdatum/spl_ldatum_exists/spl_rdatum_exists in
     * case we are doing a secondary split (see comments in gist.h).
     */
    (*sv).spl_ldatum_exists = !((*v).spl_lisnull[attno as usize]);
    (*sv).spl_rdatum_exists = !((*v).spl_risnull[attno as usize]);
    (*sv).spl_ldatum = (*v).spl_lattr[attno as usize];
    (*sv).spl_rdatum = (*v).spl_rattr[attno as usize];

    /*
     * Let the opclass-specific PickSplit method do its thing.  Note that at
     * this point we know there are no null keys in the entryvec.
     */
    FunctionCall2Coll(
        &mut (*giststate).picksplitFn[attno as usize],
        (*giststate).supportCollation[attno as usize],
        PointerGetDatum(entryvec as *const c_void),
        PointerGetDatum(sv as *const c_void),
    );

    if (*sv).spl_nleft == 0 || (*sv).spl_nright == 0 {
        /*
         * User-defined picksplit failed to create an actual split, ie it put
         * everything on the same side.  Complain but cope.
         */
        elog!(
            DEBUG1,
            "picksplit method for column {} of index \"{}\" failed",
            attno + 1,
            std::ffi::CStr::from_ptr(RelationGetRelationName(r))
                .to_string_lossy()
        );

        /*
         * Reinit GIST_SPLITVEC. Although these fields are not used by
         * genericPickSplit(), set them up for further processing
         */
        (*sv).spl_ldatum_exists = !((*v).spl_lisnull[attno as usize]);
        (*sv).spl_rdatum_exists = !((*v).spl_risnull[attno as usize]);
        (*sv).spl_ldatum = (*v).spl_lattr[attno as usize];
        (*sv).spl_rdatum = (*v).spl_rattr[attno as usize];

        /* Do a generic split */
        genericPickSplit(giststate, entryvec, sv, attno);
    } else {
        /* hack for compatibility with old picksplit API */
        if *(*sv).spl_left.add(((*sv).spl_nleft - 1) as usize) == InvalidOffsetNumber {
            *(*sv).spl_left.add(((*sv).spl_nleft - 1) as usize) =
                ((*entryvec).n - 1) as OffsetNumber;
        }
        if *(*sv).spl_right.add(((*sv).spl_nright - 1) as usize) == InvalidOffsetNumber {
            *(*sv).spl_right.add(((*sv).spl_nright - 1) as usize) =
                ((*entryvec).n - 1) as OffsetNumber;
        }
    }

    /* Clean up if PickSplit didn't take care of a secondary split */
    if (*sv).spl_ldatum_exists || (*sv).spl_rdatum_exists {
        supportSecondarySplit(
            r,
            giststate,
            attno,
            sv,
            (*v).spl_lattr[attno as usize],
            (*v).spl_rattr[attno as usize],
        );
    }

    /* emit union datums computed by PickSplit back to v arrays */
    (*v).spl_lattr[attno as usize] = (*sv).spl_ldatum;
    (*v).spl_rattr[attno as usize] = (*sv).spl_rdatum;
    (*v).spl_lisnull[attno as usize] = false;
    (*v).spl_risnull[attno as usize] = false;

    /*
     * If index columns remain, then consider whether we can improve the split
     * by using them.
     */
    (*v).spl_dontcare = null_mut();

    if attno + 1 < (*(*giststate).nonLeafTupdesc).natts {
        let NumDontCare: c_int;

        /*
         * Make a quick check to see if left and right union keys are equal;
         * if so, the split is certainly degenerate, so tell caller to
         * re-split with the next column.
         */
        if gistKeyIsEQ(giststate, attno, (*sv).spl_ldatum, (*sv).spl_rdatum) {
            return true;
        }

        /*
         * Locate don't-care tuples, if any.  If there are none, the split is
         * optimal, so just fall out and return false.
         */
        (*v).spl_dontcare = palloc0(
            core::mem::size_of::<bool>() * ((*entryvec).n + 1) as usize,
        ) as *mut bool;

        NumDontCare = findDontCares(r, giststate, (*entryvec).vector.as_mut_ptr(), v, attno);

        if NumDontCare > 0 {
            /*
             * Remove don't-cares from spl_left[] and spl_right[].
             */
            removeDontCares(
                (*sv).spl_left,
                &mut (*sv).spl_nleft,
                (*v).spl_dontcare,
            );
            removeDontCares(
                (*sv).spl_right,
                &mut (*sv).spl_nright,
                (*v).spl_dontcare,
            );

            /*
             * If all tuples on either side were don't-cares, the split is
             * degenerate, and we're best off to ignore it and split on the
             * next column.  (We used to try to press on with a secondary
             * split by forcing a random tuple on each side to be treated as
             * non-don't-care, but it seems unlikely that that technique
             * really gives a better result.  Note that we don't want to try a
             * secondary split with empty left or right primary split sides,
             * because then there is no union key on that side for the
             * PickSplit function to try to expand, so it can have no good
             * figure of merit for what it's doing.  Also note that this check
             * ensures we can't produce a bogus one-side-only split in the
             * NumDontCare == 1 special case below.)
             */
            if (*sv).spl_nleft == 0 || (*sv).spl_nright == 0 {
                (*v).spl_dontcare = null_mut();
                return true;
            }

            /*
             * Recompute union keys, considering only non-don't-care tuples.
             * NOTE: this will set union keys for remaining index columns,
             * which will cause later calls of gistUserPicksplit to pass those
             * values down to user-defined PickSplit methods with
             * spl_ldatum_exists/spl_rdatum_exists set true.
             */
            gistunionsubkey(giststate, itup, v);

            if NumDontCare == 1 {
                /*
                 * If there's only one don't-care tuple then we can't do a
                 * PickSplit on it, so just choose whether to send it left or
                 * right by comparing penalties.  We needed the
                 * gistunionsubkey step anyway so that we have appropriate
                 * union keys for figuring the penalties.
                 */
                let mut toMove: OffsetNumber;

                /* find it ... */
                toMove = FirstOffsetNumber;
                while (toMove as c_int) < (*entryvec).n {
                    if *(*v).spl_dontcare.add(toMove as usize) {
                        break;
                    }
                    toMove += 1;
                }
                Assert!((toMove as c_int) < (*entryvec).n);

                /* ... and assign it to cheaper side */
                placeOne(
                    r,
                    giststate,
                    v,
                    *itup.add((toMove - 1) as usize),
                    toMove,
                    attno + 1,
                );

                /*
                 * At this point the union keys are wrong, but we don't care
                 * because we're done splitting.  The outermost recursion
                 * level of gistSplitByKey will fix things before returning.
                 */
            } else {
                return true;
            }
        }
    }

    false
}

/*
 * simply split page in half
 */
unsafe fn gistSplitHalf(v: *mut GIST_SPLITVEC, len: c_int) {
    let mut i: c_int;

    (*v).spl_nright = 0;
    (*v).spl_nleft = 0;
    (*v).spl_left =
        palloc(len as usize * core::mem::size_of::<OffsetNumber>()) as *mut OffsetNumber;
    (*v).spl_right =
        palloc(len as usize * core::mem::size_of::<OffsetNumber>()) as *mut OffsetNumber;
    i = 1;
    while i <= len {
        if i < len / 2 {
            *(*v).spl_right.add((*v).spl_nright as usize) = i as OffsetNumber;
            (*v).spl_nright += 1;
        } else {
            *(*v).spl_left.add((*v).spl_nleft as usize) = i as OffsetNumber;
            (*v).spl_nleft += 1;
        }
        i += 1;
    }

    /* we need not compute union keys, caller took care of it */
}

/*
 * gistSplitByKey: main entry point for page-splitting algorithm
 *
 * r: index relation
 * page: page being split
 * itup: array of IndexTuples to be processed
 * len: number of IndexTuples to be processed (must be at least 2)
 * giststate: additional info about index
 * v: working state and output area
 * attno: column we are working on (zero-based index)
 *
 * Outside caller must initialize v->spl_lisnull and v->spl_risnull arrays
 * to all-true.  On return, spl_left/spl_nleft contain indexes of tuples
 * to go left, spl_right/spl_nright contain indexes of tuples to go right,
 * spl_lattr/spl_lisnull contain left-side union key values, and
 * spl_rattr/spl_risnull contain right-side union key values.  Other fields
 * in this struct are workspace for this file.
 *
 * Outside caller must pass zero for attno.  The function may internally
 * recurse to the next column by passing attno+1.
 */
pub unsafe fn gistSplitByKey(
    r: Relation,
    page: Page,
    itup: *mut IndexTuple,
    len: c_int,
    giststate: *mut GISTSTATE,
    v: *mut GistSplitVector,
    attno: c_int,
) {
    let entryvec: *mut GistEntryVector;
    let offNullTuples: *mut OffsetNumber;
    let mut nOffNullTuples: c_int = 0;
    let mut i: c_int;

    /* generate the item array, and identify tuples with null keys */
    /* note that entryvec->vector[0] goes unused in this code */
    entryvec = palloc(
        GEVHDRSZ as usize + (len + 1) as usize * core::mem::size_of::<GISTENTRY>(),
    ) as *mut GistEntryVector;
    (*entryvec).n = len + 1;
    offNullTuples =
        palloc(len as usize * core::mem::size_of::<OffsetNumber>()) as *mut OffsetNumber;

    i = 1;
    while i <= len {
        let datum: Datum;
        let mut IsNull: bool = false;

        datum = index_getattr(
            *itup.add((i - 1) as usize),
            attno + 1,
            (*giststate).leafTupdesc,
            &mut IsNull,
        );
        gistdentryinit(
            giststate,
            attno,
            (*entryvec).vector.as_mut_ptr().add(i as usize),
            datum,
            r,
            page,
            i as OffsetNumber,
            false,
            IsNull,
        );
        if IsNull {
            *offNullTuples.add(nOffNullTuples as usize) = i as OffsetNumber;
            nOffNullTuples += 1;
        }
        i += 1;
    }

    if nOffNullTuples == len {
        /*
         * Corner case: All keys in attno column are null, so just transfer
         * our attention to the next column.  If there's no next column, just
         * split page in half.
         */
        (*v).spl_risnull[attno as usize] = true;
        (*v).spl_lisnull[attno as usize] = true;

        if attno + 1 < (*(*giststate).nonLeafTupdesc).natts {
            gistSplitByKey(r, page, itup, len, giststate, v, attno + 1);
        } else {
            gistSplitHalf(&mut (*v).splitVector, len);
        }
    } else if nOffNullTuples > 0 {
        let mut j: c_int = 0;

        /*
         * We don't want to mix NULL and not-NULL keys on one page, so split
         * nulls to right page and not-nulls to left.
         */
        (*v).splitVector.spl_right = offNullTuples;
        (*v).splitVector.spl_nright = nOffNullTuples;
        (*v).spl_risnull[attno as usize] = true;

        (*v).splitVector.spl_left =
            palloc(len as usize * core::mem::size_of::<OffsetNumber>()) as *mut OffsetNumber;
        (*v).splitVector.spl_nleft = 0;
        i = 1;
        while i <= len {
            if j < (*v).splitVector.spl_nright
                && *offNullTuples.add(j as usize) == i as OffsetNumber
            {
                j += 1;
            } else {
                *(*v)
                    .splitVector
                    .spl_left
                    .add((*v).splitVector.spl_nleft as usize) = i as OffsetNumber;
                (*v).splitVector.spl_nleft += 1;
            }
            i += 1;
        }

        /* Compute union keys, unless outer recursion level will handle it */
        if attno == 0 && (*(*giststate).nonLeafTupdesc).natts == 1 {
            (*v).spl_dontcare = null_mut();
            gistunionsubkey(giststate, itup, v);
        }
    } else {
        /*
         * All keys are not-null, so apply user-defined PickSplit method
         */
        if gistUserPicksplit(r, entryvec, attno, v, itup, len, giststate) {
            /*
             * Splitting on attno column is not optimal, so consider
             * redistributing don't-care tuples according to the next column
             */
            Assert!(attno + 1 < (*(*giststate).nonLeafTupdesc).natts);

            if (*v).spl_dontcare.is_null() {
                /*
                 * This split was actually degenerate, so ignore it altogether
                 * and just split according to the next column.
                 */
                gistSplitByKey(r, page, itup, len, giststate, v, attno + 1);
            } else {
                /*
                 * Form an array of just the don't-care tuples to pass to a
                 * recursive invocation of this function for the next column.
                 */
                let newitup: *mut IndexTuple =
                    palloc(len as usize * core::mem::size_of::<IndexTuple>()) as *mut IndexTuple;
                let map: *mut OffsetNumber =
                    palloc(len as usize * core::mem::size_of::<OffsetNumber>())
                        as *mut OffsetNumber;
                let mut newlen: c_int = 0;
                let mut backupSplit: GIST_SPLITVEC;

                i = 0;
                while i < len {
                    if *(*v).spl_dontcare.add((i + 1) as usize) {
                        *newitup.add(newlen as usize) = *itup.add(i as usize);
                        *map.add(newlen as usize) = (i + 1) as OffsetNumber;
                        newlen += 1;
                    }
                    i += 1;
                }

                Assert!(newlen > 0);

                /*
                 * Make a backup copy of v->splitVector, since the recursive
                 * call will overwrite that with its own result.
                 */
                backupSplit = (*v).splitVector;
                backupSplit.spl_left =
                    palloc(core::mem::size_of::<OffsetNumber>() * len as usize)
                        as *mut OffsetNumber;
                core::ptr::copy_nonoverlapping(
                    (*v).splitVector.spl_left,
                    backupSplit.spl_left,
                    (*v).splitVector.spl_nleft as usize,
                );
                backupSplit.spl_right =
                    palloc(core::mem::size_of::<OffsetNumber>() * len as usize)
                        as *mut OffsetNumber;
                core::ptr::copy_nonoverlapping(
                    (*v).splitVector.spl_right,
                    backupSplit.spl_right,
                    (*v).splitVector.spl_nright as usize,
                );

                /* Recursively decide how to split the don't-care tuples */
                gistSplitByKey(r, page, newitup, newlen, giststate, v, attno + 1);

                /* Merge result of subsplit with non-don't-care tuples */
                i = 0;
                while i < (*v).splitVector.spl_nleft {
                    *backupSplit.spl_left.add(backupSplit.spl_nleft as usize) = *map
                        .add((*(*v).splitVector.spl_left.add(i as usize) - 1) as usize);
                    backupSplit.spl_nleft += 1;
                    i += 1;
                }
                i = 0;
                while i < (*v).splitVector.spl_nright {
                    *backupSplit.spl_right.add(backupSplit.spl_nright as usize) = *map
                        .add((*(*v).splitVector.spl_right.add(i as usize) - 1) as usize);
                    backupSplit.spl_nright += 1;
                    i += 1;
                }

                (*v).splitVector = backupSplit;
            }
        }
    }

    /*
     * If we're handling a multicolumn index, at the end of the recursion
     * recompute the left and right union datums for all index columns.  This
     * makes sure we hand back correct union datums in all corner cases,
     * including when we haven't processed all columns to start with, or when
     * a secondary split moved "don't care" tuples from one side to the other
     * (we really shouldn't assume that that didn't change the union datums).
     *
     * Note: when we're in an internal recursion (attno > 0), we do not worry
     * about whether the union datums we return with are sensible, since
     * calling levels won't care.  Also, in a single-column index, we expect
     * that PickSplit (or the special cases above) produced correct union
     * datums.
     */
    if attno == 0 && (*(*giststate).nonLeafTupdesc).natts > 1 {
        (*v).spl_dontcare = null_mut();
        gistunionsubkey(giststate, itup, v);
    }
}
