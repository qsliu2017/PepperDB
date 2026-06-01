//! Translation of postgres/src/backend/access/spgist/spgtextproc.c
//!
//! Implementation of a radix tree (compressed trie) over text for SP-GiST.
//!
//! In a text_ops SPGiST index, inner tuples can have a prefix which is the
//! common prefix of all strings indexed under that tuple.  The node labels
//! represent the next byte of the string(s) after the prefix.  To reconstruct
//! the indexed string for any index entry, concatenate the inner-tuple prefixes
//! and node labels starting at the root down to the leaf, then append the leaf
//! datum.  Node label -1 means "no more bytes after the prefix"; -2 is a dummy
//! label produced when an allTheSame tuple has to be split.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * The spgConfigOut, spgChooseIn/Out, spgPickSplitIn/Out,
//!   spgInnerConsistentIn/Out, spgLeafConsistentIn/Out structs come from
//!   access/spgist.h (NOT ported).  We define MINIMAL #[repr(C)] mirrors below
//!   containing ONLY the fields these functions touch.  spgChooseOut has a
//!   tagged-union "result" member in C carrying matchNode / addNode / splitTuple
//!   arms; the splitTuple arm is the widest, so we lay the union out as a single
//!   struct holding the union of all touched fields and access them by arm.
//!   We deliberately do NOT import the sibling spgquadtreeproc mirrors (their
//!   field sets differ); this file is self-contained.
//!
//! * varstr_cmp / strcoll / pg_newlocale_from_collation (locale-aware compare)
//!   are NOT ported.  DEVIATION: we always do a plain byte memcmp, i.e. we treat
//!   every collation as the C collation.  Consequently the collation-aware
//!   strategies (s > 10) are handled exactly like their byte-wise btree
//!   counterparts, and we never bail out of inner_consistent on "non-C collation
//!   so traverse whole tree".  text_starts_with (used by RTPrefixStrategyNumber
//!   in leaf_consistent) is also not ported; DEVIATION: we implement the prefix
//!   test by a direct byte memcmp of the query against the reconstructed value.
//!
//! * pg_cmp_s16 comes from common/int.h (crate::common::int).
//!
//! * qsort_arg with cmpNodePtr in picksplit is replaced by Rust
//!   slice::sort_by using pg_cmp_s16 on the node label byte (stable grouping by
//!   next byte is preserved).
//!
//! * SPGIST_MAX_PREFIX_LENGTH depends on BLCKSZ; we inline the default-BLCKSZ
//!   value (8192) -> Max(8192 - 258*16 - 100, 32) = 3964.
//!
//! * datumCopy comes from crate::utils::adt::datum.  cstring/text helpers and
//!   the VAR* macros come from crate::varatt.

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_VOID};
use crate::access::common::scankey::ScanKey;
use crate::access::stratnum::{
    StrategyNumber, BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessEqualStrategyNumber, BTLessStrategyNumber, RTPrefixStrategyNumber,
};
use crate::common::int::pg_cmp_s16;
use crate::utils::adt::datum::datumCopy;
use crate::varatt::{
    SET_VARSIZE, SET_VARSIZE_SHORT, VARDATA, VARDATA_ANY, VARHDRSZ, VARHDRSZ_SHORT,
    VARSIZE_ANY_EXHDR,
};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

// ===========================================================================
// Stubbed catalog/pg_type.h OIDs (catalog not consulted here).
// ===========================================================================

const TEXTOID: Oid = 25;
const INT2OID: Oid = 21;

// ===========================================================================
// varatt.h constant not ported: VARATT_SHORT_MAX == 0x7F.
// ===========================================================================

const VARATT_SHORT_MAX: c_int = 0x7F;

// ===========================================================================
// SPGIST_MAX_PREFIX_LENGTH (access/spgist_private.h-ish): depends on BLCKSZ.
// With the default BLCKSZ == 8192: Max(8192 - 258*16 - 100, 32) == 3964.
// ===========================================================================

const SPGIST_MAX_PREFIX_LENGTH: c_int = {
    let v = 8192 - 258 * 16 - 100;
    if v > 32 {
        v
    } else {
        32
    }
};

// ===========================================================================
// Collation-aware strategy handling.
//
// Strategy for a collation-aware operator on text equals the btree strategy
// plus 10.  SPG_IS_COLLATION_AWARE_STRATEGY(s): s > 10 && s != RTPrefix.
// ===========================================================================

const SPG_STRATEGY_ADDITION: StrategyNumber = 10;

#[inline]
fn SPG_IS_COLLATION_AWARE_STRATEGY(s: StrategyNumber) -> bool {
    s > SPG_STRATEGY_ADDITION && s != RTPrefixStrategyNumber
}

// ===========================================================================
// Minimal access/spgist.h struct mirrors (only the touched fields).
// ===========================================================================

/// `spgConfigOut` (touched fields only).
#[repr(C)]
pub struct spgConfigOut {
    pub prefixType: Oid,
    pub labelType: Oid,
    pub canReturnData: bool,
    pub longValuesOK: bool,
}

/// Result-type tags for spgChooseOut.resultType (access/spgist.h `spgChooseResultType`).
const spgMatchNode: c_int = 1;
const spgAddNode: c_int = 2;
const spgSplitTuple: c_int = 3;

/// `spgChooseIn` (touched fields only).
#[repr(C)]
pub struct spgChooseIn {
    pub datum: Datum,
    pub leafDatum: Datum,
    pub level: c_int,
    pub allTheSame: bool,
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub nNodes: c_int,
    pub nodeLabels: *mut Datum,
}

/// `spgChooseOut`.  C uses a tagged union `result` with matchNode / addNode /
/// splitTuple arms.  We flatten the union into one struct carrying every field
/// the three arms touch; only the fields valid for the chosen `resultType` are
/// meaningful.  Layout note: this is wider than the real union, but the SP-GiST
/// core reads it per-arm by `resultType`, and we only ever construct one arm at
/// a time, so the trailing fields are simply ignored by the consumer for the
/// non-split arms.
#[repr(C)]
pub struct spgChooseOut {
    pub resultType: c_int,

    // matchNode arm
    pub matchNode_nodeN: c_int,
    pub matchNode_levelAdd: c_int,
    pub matchNode_restDatum: Datum,

    // addNode arm
    pub addNode_nodeLabel: Datum,
    pub addNode_nodeN: c_int,

    // splitTuple arm
    pub splitTuple_prefixHasPrefix: bool,
    pub splitTuple_prefixPrefixDatum: Datum,
    pub splitTuple_prefixNNodes: c_int,
    pub splitTuple_prefixNodeLabels: *mut Datum,
    pub splitTuple_childNodeN: c_int,
    pub splitTuple_postfixHasPrefix: bool,
    pub splitTuple_postfixPrefixDatum: Datum,
}

/// `spgPickSplitIn` (touched fields only).
#[repr(C)]
pub struct spgPickSplitIn {
    pub nTuples: c_int,
    pub datums: *mut Datum,
    pub level: c_int,
}

/// `spgPickSplitOut` (touched fields only).
#[repr(C)]
pub struct spgPickSplitOut {
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub nNodes: c_int,
    pub nodeLabels: *mut Datum,
    pub mapTuplesToNodes: *mut c_int,
    pub leafTupleDatums: *mut Datum,
}

/// `spgInnerConsistentIn` (touched fields only).
#[repr(C)]
pub struct spgInnerConsistentIn {
    pub scankeys: ScanKey,
    pub nkeys: c_int,
    pub reconstructedValue: Datum,
    pub level: c_int,
    pub hasPrefix: bool,
    pub prefixDatum: Datum,
    pub nNodes: c_int,
    pub nodeLabels: *mut Datum,
}

/// `spgInnerConsistentOut` (touched fields only).
#[repr(C)]
pub struct spgInnerConsistentOut {
    pub nNodes: c_int,
    pub nodeNumbers: *mut c_int,
    pub levelAdds: *mut c_int,
    pub reconstructedValues: *mut Datum,
}

/// `spgLeafConsistentIn` (touched fields only).
#[repr(C)]
pub struct spgLeafConsistentIn {
    pub scankeys: ScanKey,
    pub nkeys: c_int,
    pub reconstructedValue: Datum,
    pub level: c_int,
    pub leafDatum: Datum,
}

/// `spgLeafConsistentOut` (touched fields only).
#[repr(C)]
pub struct spgLeafConsistentOut {
    pub leafValue: Datum,
    pub recheck: bool,
}

/// `struct spgNodePtr` -- for sorting values in picksplit.
#[derive(Clone, Copy)]
struct spgNodePtr {
    d: Datum,
    i: c_int,
    c: i16,
}

// ===========================================================================
// text Datum helpers (varatt-based mirrors of the C macros used here).
// ===========================================================================

/// `DatumGetTextPP(d)` -- text is a varlena; we treat it as a `*char` header.
/// (No detoasting: every value reaching these functions is already in line.)
#[inline]
unsafe fn DatumGetTextPP(d: Datum) -> *mut c_char {
    DatumGetPointer(d)
}

/// `PointerGetDatum` for a text* (`*mut c_char`).
#[inline]
unsafe fn TextPGetDatum(p: *mut c_char) -> Datum {
    PointerGetDatum(p as *const c_void)
}

// ===========================================================================
// spgtextproc.c
// ===========================================================================

/// `Datum spg_text_config(PG_FUNCTION_ARGS)`
pub unsafe fn spg_text_config(fcinfo: FunctionCallInfo) -> Datum {
    // spgConfigIn *cfgin = (spgConfigIn *) PG_GETARG_POINTER(0);
    let cfg = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgConfigOut;

    (*cfg).prefixType = TEXTOID;
    (*cfg).labelType = INT2OID;
    (*cfg).canReturnData = true;
    (*cfg).longValuesOK = true; // suffixing will shorten long values
    PG_RETURN_VOID!();
}

/// `static Datum formTextDatum(const char *data, int datalen)`
///
/// Form a text datum from the given not-necessarily-null-terminated string,
/// using short varlena header format if possible.
unsafe fn formTextDatum(data: *const c_char, datalen: c_int) -> Datum {
    let p = palloc((datalen + VARHDRSZ) as Size) as *mut c_char;

    if datalen + VARHDRSZ_SHORT <= VARATT_SHORT_MAX {
        SET_VARSIZE_SHORT(p, datalen + VARHDRSZ_SHORT);
        if datalen != 0 {
            memcpy(
                p.add(VARHDRSZ_SHORT as usize) as *mut c_void,
                data as *const c_void,
                datalen as usize,
            );
        }
    } else {
        SET_VARSIZE(p, datalen + VARHDRSZ);
        memcpy(
            p.add(VARHDRSZ as usize) as *mut c_void,
            data as *const c_void,
            datalen as usize,
        );
    }

    TextPGetDatum(p)
}

/// `static int commonPrefix(const char *a, const char *b, int lena, int lenb)`
///
/// Find the length of the common prefix of a and b.
unsafe fn commonPrefix(
    mut a: *const c_char,
    mut b: *const c_char,
    lena: c_int,
    lenb: c_int,
) -> c_int {
    let mut i: c_int = 0;

    while i < lena && i < lenb && *a == *b {
        a = a.add(1);
        b = b.add(1);
        i += 1;
    }

    i
}

/// `static bool searchChar(Datum *nodeLabels, int nNodes, int16 c, int *i)`
///
/// Binary search an array of int16 datums for a match to c.  On success,
/// returns true and `*i` is the match location; on failure, returns false and
/// `*i` is where to insert.
unsafe fn searchChar(nodeLabels: *mut Datum, nNodes: c_int, c: i16, i: *mut c_int) -> bool {
    let mut StopLow: c_int = 0;
    let mut StopHigh: c_int = nNodes;

    while StopLow < StopHigh {
        let StopMiddle = (StopLow + StopHigh) >> 1;
        let middle = DatumGetInt16(*nodeLabels.add(StopMiddle as usize));

        if c < middle {
            StopHigh = StopMiddle;
        } else if c > middle {
            StopLow = StopMiddle + 1;
        } else {
            *i = StopMiddle;
            return true;
        }
    }

    *i = StopHigh;
    false
}

/// `Datum spg_text_choose(PG_FUNCTION_ARGS)`
pub unsafe fn spg_text_choose(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgChooseIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgChooseOut;
    let inText = DatumGetTextPP((*in_).datum);
    let inStr = VARDATA_ANY(inText);
    let inSize = VARSIZE_ANY_EXHDR(inText) as c_int;
    let mut prefixStr: *mut c_char = null_mut();
    let mut prefixSize: c_int = 0;
    let mut commonLen: c_int = 0;
    let mut nodeChar: i16 = 0;
    let mut i: c_int = 0;

    // Check for prefix match, set nodeChar to first byte after prefix
    if (*in_).hasPrefix {
        let prefixText = DatumGetTextPP((*in_).prefixDatum);

        prefixStr = VARDATA_ANY(prefixText);
        prefixSize = VARSIZE_ANY_EXHDR(prefixText) as c_int;

        commonLen = commonPrefix(
            inStr.add((*in_).level as usize),
            prefixStr,
            inSize - (*in_).level,
            prefixSize,
        );

        if commonLen == prefixSize {
            if inSize - (*in_).level > commonLen {
                nodeChar =
                    *(inStr.add(((*in_).level + commonLen) as usize) as *const u8) as i16;
            } else {
                nodeChar = -1;
            }
        } else {
            // Must split tuple because incoming value doesn't match prefix
            (*out).resultType = spgSplitTuple;

            if commonLen == 0 {
                (*out).splitTuple_prefixHasPrefix = false;
            } else {
                (*out).splitTuple_prefixHasPrefix = true;
                (*out).splitTuple_prefixPrefixDatum = formTextDatum(prefixStr, commonLen);
            }
            (*out).splitTuple_prefixNNodes = 1;
            (*out).splitTuple_prefixNodeLabels =
                palloc(core::mem::size_of::<Datum>()) as *mut Datum;
            *(*out).splitTuple_prefixNodeLabels.add(0) = Int16GetDatum(
                *(prefixStr.add(commonLen as usize) as *const u8) as i16,
            );

            (*out).splitTuple_childNodeN = 0;

            if prefixSize - commonLen == 1 {
                (*out).splitTuple_postfixHasPrefix = false;
            } else {
                (*out).splitTuple_postfixHasPrefix = true;
                (*out).splitTuple_postfixPrefixDatum = formTextDatum(
                    prefixStr.add((commonLen + 1) as usize),
                    prefixSize - commonLen - 1,
                );
            }

            PG_RETURN_VOID!();
        }
    } else if inSize > (*in_).level {
        nodeChar = *(inStr.add((*in_).level as usize) as *const u8) as i16;
    } else {
        nodeChar = -1;
    }

    // Look up nodeChar in the node label array
    if searchChar((*in_).nodeLabels, (*in_).nNodes, nodeChar, &mut i) {
        /*
         * Descend to existing node.  (If in->allTheSame, the core code will
         * ignore our nodeN specification here, but that's OK.  We still have to
         * provide the correct levelAdd and restDatum values, and those are the
         * same regardless of which node gets chosen by core.)
         */
        (*out).resultType = spgMatchNode;
        (*out).matchNode_nodeN = i;
        let mut levelAdd = commonLen;
        if nodeChar >= 0 {
            levelAdd += 1;
        }
        (*out).matchNode_levelAdd = levelAdd;
        if inSize - (*in_).level - levelAdd > 0 {
            (*out).matchNode_restDatum = formTextDatum(
                inStr.add(((*in_).level + levelAdd) as usize),
                inSize - (*in_).level - levelAdd,
            );
        } else {
            (*out).matchNode_restDatum = formTextDatum(null_mut(), 0);
        }
    } else if (*in_).allTheSame {
        /*
         * Can't use AddNode action, so split the tuple.  The upper tuple has the
         * same prefix as before and uses a dummy node label -2 for the lower
         * tuple.  The lower tuple has no prefix and the same node labels as the
         * original tuple.
         */
        (*out).resultType = spgSplitTuple;
        (*out).splitTuple_prefixHasPrefix = (*in_).hasPrefix;
        (*out).splitTuple_prefixPrefixDatum = (*in_).prefixDatum;
        (*out).splitTuple_prefixNNodes = 1;
        (*out).splitTuple_prefixNodeLabels = palloc(core::mem::size_of::<Datum>()) as *mut Datum;
        *(*out).splitTuple_prefixNodeLabels.add(0) = Int16GetDatum(-2);
        (*out).splitTuple_childNodeN = 0;
        (*out).splitTuple_postfixHasPrefix = false;
    } else {
        // Add a node for the not-previously-seen nodeChar value
        (*out).resultType = spgAddNode;
        (*out).addNode_nodeLabel = Int16GetDatum(nodeChar);
        (*out).addNode_nodeN = i;
    }

    PG_RETURN_VOID!();
}

/// `Datum spg_text_picksplit(PG_FUNCTION_ARGS)`
pub unsafe fn spg_text_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgPickSplitIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgPickSplitOut;
    let text0 = DatumGetTextPP(*(*in_).datums.add(0));
    let mut commonLen: c_int;

    // Identify longest common prefix, if any
    commonLen = VARSIZE_ANY_EXHDR(text0) as c_int;
    let mut i: c_int = 1;
    while i < (*in_).nTuples && commonLen > 0 {
        let texti = DatumGetTextPP(*(*in_).datums.add(i as usize));
        let tmp = commonPrefix(
            VARDATA_ANY(text0),
            VARDATA_ANY(texti),
            VARSIZE_ANY_EXHDR(text0) as c_int,
            VARSIZE_ANY_EXHDR(texti) as c_int,
        );

        if tmp < commonLen {
            commonLen = tmp;
        }
        i += 1;
    }

    // Limit the prefix length, if necessary, to fit on a page.
    if commonLen > SPGIST_MAX_PREFIX_LENGTH {
        commonLen = SPGIST_MAX_PREFIX_LENGTH;
    }

    // Set node prefix to be that string, if it's not empty
    if commonLen == 0 {
        (*out).hasPrefix = false;
    } else {
        (*out).hasPrefix = true;
        (*out).prefixDatum = formTextDatum(VARDATA_ANY(text0), commonLen);
    }

    // Extract the node label (first non-common byte) from each value
    let nodes =
        palloc(core::mem::size_of::<spgNodePtr>() * (*in_).nTuples as usize) as *mut spgNodePtr;

    i = 0;
    while i < (*in_).nTuples {
        let texti = DatumGetTextPP(*(*in_).datums.add(i as usize));
        let node = nodes.add(i as usize);

        if commonLen < VARSIZE_ANY_EXHDR(texti) as c_int {
            (*node).c = *(VARDATA_ANY(texti).add(commonLen as usize) as *const u8) as i16;
        } else {
            (*node).c = -1; // use -1 if string is all common
        }
        (*node).i = i;
        (*node).d = *(*in_).datums.add(i as usize);
        i += 1;
    }

    /*
     * Sort by label values so that we can group the values into nodes.  This
     * also ensures that the nodes are ordered by label value, allowing the use
     * of binary search in searchChar.  (qsort_arg with cmpNodePtr -> Rust
     * slice::sort_by on pg_cmp_s16 of the label byte.)
     */
    let nodes_slice = core::slice::from_raw_parts_mut(nodes, (*in_).nTuples as usize);
    nodes_slice.sort_by(|a, b| match pg_cmp_s16(a.c, b.c) {
        x if x < 0 => core::cmp::Ordering::Less,
        x if x > 0 => core::cmp::Ordering::Greater,
        _ => core::cmp::Ordering::Equal,
    });

    // And emit results
    (*out).nNodes = 0;
    (*out).nodeLabels = palloc(core::mem::size_of::<Datum>() * (*in_).nTuples as usize) as *mut Datum;
    (*out).mapTuplesToNodes =
        palloc(core::mem::size_of::<c_int>() * (*in_).nTuples as usize) as *mut c_int;
    (*out).leafTupleDatums =
        palloc(core::mem::size_of::<Datum>() * (*in_).nTuples as usize) as *mut Datum;

    i = 0;
    while i < (*in_).nTuples {
        let node = nodes.add(i as usize);
        let texti = DatumGetTextPP((*node).d);
        let leafD: Datum;

        if i == 0 || (*node).c != (*nodes.add((i - 1) as usize)).c {
            *(*out).nodeLabels.add((*out).nNodes as usize) = Int16GetDatum((*node).c);
            (*out).nNodes += 1;
        }

        if commonLen < VARSIZE_ANY_EXHDR(texti) as c_int {
            leafD = formTextDatum(
                VARDATA_ANY(texti).add((commonLen + 1) as usize),
                VARSIZE_ANY_EXHDR(texti) as c_int - commonLen - 1,
            );
        } else {
            leafD = formTextDatum(null_mut(), 0);
        }

        *(*out).leafTupleDatums.add((*node).i as usize) = leafD;
        *(*out).mapTuplesToNodes.add((*node).i as usize) = (*out).nNodes - 1;
        i += 1;
    }

    PG_RETURN_VOID!();
}

/// `Datum spg_text_inner_consistent(PG_FUNCTION_ARGS)`
pub unsafe fn spg_text_inner_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgInnerConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgInnerConsistentOut;
    // DEVIATION: locale not ported; treat collation as C (collate_is_c = true).
    let collate_is_c = true;
    let reconstructedValue: *mut c_char;
    let reconstrText: *mut c_char;
    let maxReconstrLen: c_int;
    let mut prefixText: *mut c_char = null_mut();
    let mut prefixSize: c_int = 0;
    let mut i: c_int;

    /*
     * Reconstruct values represented at this tuple, including parent data,
     * prefix of this tuple if any, and the node label if it's non-dummy.
     * in->level should be the length of the previously reconstructed value, and
     * the number of bytes added here is prefixSize or prefixSize + 1.
     *
     * Note: we assume that in->reconstructedValue isn't toasted and doesn't have
     * a short varlena header.
     */
    reconstructedValue = DatumGetPointer((*in_).reconstructedValue);

    maxReconstrLen = {
        let mut m = (*in_).level + 1;
        if (*in_).hasPrefix {
            prefixText = DatumGetTextPP((*in_).prefixDatum);
            prefixSize = VARSIZE_ANY_EXHDR(prefixText) as c_int;
            m += prefixSize;
        }
        m
    };

    reconstrText = palloc((VARHDRSZ + maxReconstrLen) as Size) as *mut c_char;
    SET_VARSIZE(reconstrText, VARHDRSZ + maxReconstrLen);

    if (*in_).level != 0 {
        memcpy(
            VARDATA(reconstrText) as *mut c_void,
            VARDATA(reconstructedValue) as *const c_void,
            (*in_).level as usize,
        );
    }
    if prefixSize != 0 {
        memcpy(
            VARDATA(reconstrText).add((*in_).level as usize) as *mut c_void,
            VARDATA_ANY(prefixText) as *const c_void,
            prefixSize as usize,
        );
    }
    // last byte of reconstrText will be filled in below

    /*
     * Scan the child nodes.  For each one, complete the reconstructed value and
     * see if it's consistent with the query.  If so, emit an entry into the
     * output arrays.
     */
    (*out).nodeNumbers = palloc(core::mem::size_of::<c_int>() * (*in_).nNodes as usize) as *mut c_int;
    (*out).levelAdds = palloc(core::mem::size_of::<c_int>() * (*in_).nNodes as usize) as *mut c_int;
    (*out).reconstructedValues =
        palloc(core::mem::size_of::<Datum>() * (*in_).nNodes as usize) as *mut Datum;
    (*out).nNodes = 0;

    i = 0;
    while i < (*in_).nNodes {
        let nodeChar = DatumGetInt16(*(*in_).nodeLabels.add(i as usize));
        let thisLen: c_int;
        let mut res = true;
        let mut j: c_int;

        // If nodeChar is a dummy value, don't include it in data
        if nodeChar <= 0 {
            thisLen = maxReconstrLen - 1;
        } else {
            *(VARDATA(reconstrText).add((maxReconstrLen - 1) as usize) as *mut u8) =
                nodeChar as u8;
            thisLen = maxReconstrLen;
        }

        j = 0;
        while j < (*in_).nkeys {
            let sk = (*in_).scankeys.add(j as usize);
            let mut strategy = (*sk).sk_strategy;

            /*
             * If it's a collation-aware operator, but the collation is C, we can
             * treat it as non-collation-aware.  With non-C collation we need to
             * traverse the whole tree, so there's no point in checking here.
             * (DEVIATION: collate_is_c is hard-coded true, so the `else continue`
             * branch is dead; every collation-aware strategy is demoted.)
             */
            if SPG_IS_COLLATION_AWARE_STRATEGY(strategy) {
                if collate_is_c {
                    strategy -= SPG_STRATEGY_ADDITION;
                } else {
                    j += 1;
                    continue;
                }
            }

            let inText = DatumGetTextPP((*sk).sk_argument);
            let inSize = VARSIZE_ANY_EXHDR(inText) as c_int;

            let r = memcmp(
                VARDATA(reconstrText) as *const c_void,
                VARDATA_ANY(inText) as *const c_void,
                core::cmp::min(inSize, thisLen) as usize,
            );

            match strategy {
                s if s == BTLessStrategyNumber || s == BTLessEqualStrategyNumber => {
                    if r > 0 {
                        res = false;
                    }
                }
                s if s == BTEqualStrategyNumber => {
                    if r != 0 || inSize < thisLen {
                        res = false;
                    }
                }
                s if s == BTGreaterEqualStrategyNumber || s == BTGreaterStrategyNumber => {
                    if r < 0 {
                        res = false;
                    }
                }
                s if s == RTPrefixStrategyNumber => {
                    if r != 0 {
                        res = false;
                    }
                }
                _ => {
                    elog!(
                        ERROR,
                        "unrecognized strategy number: {}",
                        (*sk).sk_strategy
                    );
                    #[allow(unreachable_code)]
                    {
                        unreachable!()
                    }
                }
            }

            if !res {
                break; // no need to consider remaining conditions
            }
            j += 1;
        }

        if res {
            *(*out).nodeNumbers.add((*out).nNodes as usize) = i;
            *(*out).levelAdds.add((*out).nNodes as usize) = thisLen - (*in_).level;
            SET_VARSIZE(reconstrText, VARHDRSZ + thisLen);
            *(*out).reconstructedValues.add((*out).nNodes as usize) =
                datumCopy(TextPGetDatum(reconstrText), false, -1);
            (*out).nNodes += 1;
        }
        i += 1;
    }

    PG_RETURN_VOID!();
}

/// `Datum spg_text_leaf_consistent(PG_FUNCTION_ARGS)`
pub unsafe fn spg_text_leaf_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let in_ = PG_GETARG_POINTER!(fcinfo, 0) as *mut spgLeafConsistentIn;
    let out = PG_GETARG_POINTER!(fcinfo, 1) as *mut spgLeafConsistentOut;
    let level = (*in_).level;
    let leafValue: *mut c_char;
    let mut reconstrValue: *mut c_char = null_mut();
    let fullValue: *mut c_char;
    let fullLen: c_int;
    let mut res: bool;
    let mut j: c_int;

    // all tests are exact
    (*out).recheck = false;

    leafValue = DatumGetTextPP((*in_).leafDatum);

    // As above, in->reconstructedValue isn't toasted or short.
    if !DatumGetPointer((*in_).reconstructedValue).is_null() {
        reconstrValue = DatumGetPointer((*in_).reconstructedValue);
    }

    // Reconstruct the full string represented by this leaf tuple
    fullLen = level + VARSIZE_ANY_EXHDR(leafValue) as c_int;
    if VARSIZE_ANY_EXHDR(leafValue) == 0 && level > 0 {
        fullValue = VARDATA(reconstrValue);
        (*out).leafValue = TextPGetDatum(reconstrValue);
    } else {
        let fullText = palloc((VARHDRSZ + fullLen) as Size) as *mut c_char;

        SET_VARSIZE(fullText, VARHDRSZ + fullLen);
        fullValue = VARDATA(fullText);
        if level != 0 {
            memcpy(
                fullValue as *mut c_void,
                VARDATA(reconstrValue) as *const c_void,
                level as usize,
            );
        }
        if VARSIZE_ANY_EXHDR(leafValue) > 0 {
            memcpy(
                fullValue.add(level as usize) as *mut c_void,
                VARDATA_ANY(leafValue) as *const c_void,
                VARSIZE_ANY_EXHDR(leafValue) as usize,
            );
        }
        (*out).leafValue = TextPGetDatum(fullText);
    }

    // Perform the required comparison(s)
    res = true;
    j = 0;
    while j < (*in_).nkeys {
        let sk = (*in_).scankeys.add(j as usize);
        let mut strategy = (*sk).sk_strategy;
        let query = DatumGetTextPP((*sk).sk_argument);
        let queryLen = VARSIZE_ANY_EXHDR(query) as c_int;
        let mut r: c_int;

        if strategy == RTPrefixStrategyNumber {
            /*
             * If level >= length of query then reconstrValue must begin with the
             * query (prefix) string, so we don't need to check it again.
             *
             * DEVIATION: text_starts_with (DirectFunctionCall2Coll) is not
             * ported.  We implement the prefix test as a byte memcmp of the
             * query against the full reconstructed value.
             */
            res = (level >= queryLen)
                || (fullLen >= queryLen
                    && memcmp(
                        fullValue as *const c_void,
                        VARDATA_ANY(query) as *const c_void,
                        queryLen as usize,
                    ) == 0);

            if !res {
                break; // no need to consider remaining conditions
            }

            j += 1;
            continue;
        }

        if SPG_IS_COLLATION_AWARE_STRATEGY(strategy) {
            // Collation-aware comparison (DEVIATION: byte compare, treat as C).
            strategy -= SPG_STRATEGY_ADDITION;
        }

        // Non-collation-aware (byte) comparison.
        r = memcmp(
            fullValue as *const c_void,
            VARDATA_ANY(query) as *const c_void,
            core::cmp::min(queryLen, fullLen) as usize,
        );

        if r == 0 {
            if queryLen > fullLen {
                r = -1;
            } else if queryLen < fullLen {
                r = 1;
            }
        }

        match strategy {
            s if s == BTLessStrategyNumber => res = r < 0,
            s if s == BTLessEqualStrategyNumber => res = r <= 0,
            s if s == BTEqualStrategyNumber => res = r == 0,
            s if s == BTGreaterEqualStrategyNumber => res = r >= 0,
            s if s == BTGreaterStrategyNumber => res = r > 0,
            _ => {
                elog!(
                    ERROR,
                    "unrecognized strategy number: {}",
                    (*sk).sk_strategy
                );
                #[allow(unreachable_code)]
                {
                    res = false;
                }
            }
        }

        if !res {
            break; // no need to consider remaining conditions
        }
        j += 1;
    }

    PG_RETURN_BOOL!(res);
}

// ===========================================================================
// Tests: the byte-radix helpers (commonPrefix, searchChar, formTextDatum
// round-trip) are the REAL testable core and need no SP-GiST framework setup.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // commonPrefix: longest common byte prefix length, bounded by both lengths.
    #[test]
    fn common_prefix_basic() {
        unsafe {
            let a = b"hello";
            let b = b"help";
            assert_eq!(
                commonPrefix(
                    a.as_ptr() as *const c_char,
                    b.as_ptr() as *const c_char,
                    a.len() as c_int,
                    b.len() as c_int,
                ),
                3
            ); // "hel"

            let c = b"abc";
            let d = b"xyz";
            assert_eq!(
                commonPrefix(
                    c.as_ptr() as *const c_char,
                    d.as_ptr() as *const c_char,
                    3,
                    3,
                ),
                0
            );

            // bounded by shorter length: "ab" vs "abcdef" -> 2
            let e = b"ab";
            let f = b"abcdef";
            assert_eq!(
                commonPrefix(
                    e.as_ptr() as *const c_char,
                    f.as_ptr() as *const c_char,
                    2,
                    6,
                ),
                2
            );
        }
    }

    // searchChar: binary search over sorted int16 node labels.
    #[test]
    fn search_char_hit_and_miss() {
        unsafe {
            // labels sorted ascending: -1, 97('a'), 99('c'), 101('e')
            let labels: [Datum; 4] = [
                Int16GetDatum(-1),
                Int16GetDatum(97),
                Int16GetDatum(99),
                Int16GetDatum(101),
            ];
            let mut idx: c_int = -42;

            assert!(searchChar(labels.as_ptr() as *mut Datum, 4, 99, &mut idx));
            assert_eq!(idx, 2);

            assert!(searchChar(labels.as_ptr() as *mut Datum, 4, -1, &mut idx));
            assert_eq!(idx, 0);

            // miss: 98('b') would insert at index 2 (between 97 and 99)
            assert!(!searchChar(labels.as_ptr() as *mut Datum, 4, 98, &mut idx));
            assert_eq!(idx, 2);

            // miss above all: 200 inserts at end
            assert!(!searchChar(labels.as_ptr() as *mut Datum, 4, 200, &mut idx));
            assert_eq!(idx, 4);
        }
    }

    // formTextDatum round-trip: bytes in -> text Datum -> VARDATA_ANY bytes out.
    #[test]
    fn form_text_datum_roundtrip_short_and_long() {
        unsafe {
            // short payload uses short header; data must read back intact
            let s = b"radix";
            let d = formTextDatum(s.as_ptr() as *const c_char, s.len() as c_int);
            let p = DatumGetPointer(d);
            assert_eq!(VARSIZE_ANY_EXHDR(p) as usize, s.len());
            let out = core::slice::from_raw_parts(VARDATA_ANY(p) as *const u8, s.len());
            assert_eq!(out, s);

            // empty payload
            let d0 = formTextDatum(null_mut(), 0);
            let p0 = DatumGetPointer(d0);
            assert_eq!(VARSIZE_ANY_EXHDR(p0) as usize, 0);

            // long payload (> VARATT_SHORT_MAX-1 bytes) takes the 4B-header path
            let big = vec![b'z'; 200];
            let db = formTextDatum(big.as_ptr() as *const c_char, big.len() as c_int);
            let pb = DatumGetPointer(db);
            assert_eq!(VARSIZE_ANY_EXHDR(pb) as usize, big.len());
            let outb = core::slice::from_raw_parts(VARDATA_ANY(pb) as *const u8, big.len());
            assert_eq!(outb, &big[..]);
        }
    }

    // Strategy byte ordering used by leaf_consistent: a < b under BTLess.
    #[test]
    fn strategy_byte_less() {
        // "apple" < "banana": memcmp gives r<0, BTLessStrategyNumber -> true.
        let a = b"apple";
        let b = b"banana";
        let n = core::cmp::min(a.len(), b.len());
        let mut r = unsafe {
            memcmp(
                a.as_ptr() as *const c_void,
                b.as_ptr() as *const c_void,
                n,
            )
        };
        if r == 0 {
            if b.len() > a.len() {
                r = -1;
            } else if b.len() < a.len() {
                r = 1;
            }
        }
        // BTLess: res = r < 0
        assert!(r < 0);
        // BTGreater on the same pair would be false
        assert!(!(r > 0));
    }
}
