//! Translation of postgres/src/backend/utils/adt/levenshtein.c
//!                (+ the SQL-callable wrappers from
//!                   postgres/contrib/fuzzystrmatch/fuzzystrmatch.c)
//!
//! Levenshtein edit-distance metric between two `text` values.
//!
//! Original author:  Joe Conway <mail@joeconway.com>
//! Configurable penalty costs extension by Volkan YAZICI <volkan.yazici@gmail.com>.
//!
//! Portions Copyright (c) 2001-2025, PostgreSQL Global Development Group
//!
//! INCLUDE-TEMPLATE MERGE
//! ----------------------
//! The C source is `#include`d by varlena.c TWICE via the
//! `#ifdef LEVENSHTEIN_LESS_EQUAL` trick: once to emit `varstr_levenshtein`
//! (plain) and once to emit `varstr_levenshtein_less_equal` (with a `max_d`
//! early-abort bound).  We collapse BOTH instantiations into ONE generic
//!
//!   unsafe fn levenshtein_internal(src, slen, dst, tlen,
//!                                  ins_c, del_c, sub_c, max_d, trusted) -> c_int
//!
//! where `max_d < 0` selects the plain behavior (no bound).  The start_column/
//! stop_column machinery is always present; with `max_d < 0` it degenerates
//! exactly to the plain variant's `START_COLUMN=0` / `STOP_COLUMN=m` (because
//! stop_column is initialized to m+1 from the pre-increment m and never slid).
//! `rest_of_char_same` (the inline helper in varlena.c) is inlined here.
//!
//! MULTIBYTE NOTE
//! --------------
//! The C counts characters via `pg_mbstrlen_with_len`, then caches per-character
//! byte lengths using `pg_mblen_range(cp, send)` (bounded by an END pointer).
//! crate::mb::mbutils only exposes `pg_mblen_with_len(mbstr, limit)` (bounded by a
//! BYTE COUNT); since `send = source + slen` and `tend = target + tlen`, the
//! remaining byte limit `send - cp` / `tend - y` is exactly the C end-pointer
//! bound, so we use `pg_mblen_with_len` with that computed limit (faithful, and
//! `pg_mblen_range` is currently stubbed in mbutils anyway).
//!
//! `#include`s mapped:
//!   - varatt.h VAR* macros          -> crate::varatt (VARDATA_ANY / VARSIZE_ANY_EXHDR)
//!   - mb/pg_wchar.h                  -> crate::mb::mbutils (pg_mbstrlen_with_len /
//!                                       pg_mblen_with_len)
//!   - utils/fmgrprotos.h             -> crate::utils::fmgr; <string.h> not needed.
//!
//! Self-contained; nothing stubbed.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;
use crate::{PG_GETARG_DATUM, PG_GETARG_INT32, PG_RETURN_INT32};
use crate::c::text;
// pg_database_encoding_max_length() is referenced only conceptually (the
// single-byte fast paths live inside pg_mbstrlen_with_len / pg_mblen_with_len),
// so it is not imported here.
use crate::mb::mbutils::{pg_mblen_with_len, pg_mbstrlen_with_len};
use core::ffi::{c_char, c_int, c_void};

/* errcodes.h classification (the errcode() shim ignores the value). */
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

const MAX_LEVENSHTEIN_STRLEN: c_int = 255;

/*
 * Helper function for Levenshtein distance functions (varlena.c
 * rest_of_char_same).  Faster than memcmp(), for this use case.
 *
 * # Safety
 * `s1`/`s2` are readable for at least `len` bytes.
 */
#[inline]
unsafe fn rest_of_char_same(s1: *const c_char, s2: *const c_char, mut len: c_int) -> bool {
    while len > 0 {
        len -= 1;
        if *s1.add(len as usize) != *s2.add(len as usize) {
            return false;
        }
    }
    true
}

/*
 * Calculates Levenshtein distance metric between supplied strings, which are
 * not necessarily null-terminated.
 *
 * source: source string, of length slen bytes.
 * target: target string, of length tlen bytes.
 * ins_c, del_c, sub_c: costs to charge for character insertion, deletion,
 *		and substitution respectively; (1, 1, 1) costs suffice for common
 *		cases, but your mileage may vary.
 * max_d: if >= 0, maximum distance we care about; if < 0, compute the exact
 *		distance with no early-abort bound (the plain varstr_levenshtein
 *		behavior).  See below.
 * trusted: caller is trusted and need not obey MAX_LEVENSHTEIN_STRLEN.
 *
 * This is the merged body of varstr_levenshtein and
 * varstr_levenshtein_less_equal: the `#ifdef LEVENSHTEIN_LESS_EQUAL` paths are
 * taken at runtime whenever max_d >= 0.  See the file header for why the merge
 * is behavior-preserving for max_d < 0.
 *
 * One way to compute Levenshtein distance is to incrementally construct
 * an (m+1)x(n+1) matrix where cell (i, j) represents the minimum number
 * of operations required to transform the first i characters of s into
 * the first j characters of t.  The last column of the final row is the
 * answer.
 *
 * We use that algorithm here with some modification.  In lieu of holding
 * the entire array in memory at once, we'll just use two arrays of size
 * m+1 for storing accumulated values. At each step one array represents
 * the "previous" row and one is the "current" row of the notional large
 * array.
 *
 * If max_d >= 0, we only need to provide an accurate answer when that answer
 * is less than or equal to max_d.  From any cell in the matrix, there is
 * theoretical "minimum residual distance" from that cell to the last column
 * of the final row.  This minimum residual distance is zero when the
 * untransformed portions of the strings are of equal length (because we might
 * get lucky and find all the remaining characters matching) and is otherwise
 * based on the minimum number of insertions or deletions needed to make them
 * equal length.  The residual distance grows as we move toward the upper
 * right or lower left corners of the matrix.  When the max_d bound is
 * usefully tight, we can use this property to avoid computing the entirety
 * of each row; instead, we maintain a start_column and stop_column that
 * identify the portion of the matrix close to the diagonal which can still
 * affect the final answer.
 *
 * # Safety
 * `source`/`target` are readable for `slen`/`tlen` bytes respectively.
 */
unsafe fn levenshtein_internal(
    mut source: *const c_char,
    slen: c_int,
    target: *const c_char,
    tlen: c_int,
    ins_c: c_int,
    del_c: c_int,
    mut sub_c: c_int,
    mut max_d: c_int,
    trusted: bool,
) -> c_int {
    let mut m: c_int;
    let n: c_int;
    let mut prev: *mut c_int;
    let mut curr: *mut c_int;
    let mut s_char_len: *mut c_int = null_mut();
    let mut y: *const c_char;
    let send: *const c_char = source.add(slen as usize);
    let tend: *const c_char = target.add(tlen as usize);

    /*
     * For varstr_levenshtein_less_equal, we have real variables called
     * start_column and stop_column; otherwise (max_d < 0) they're just 0 and
     * m+1, which the macros START_COLUMN/STOP_COLUMN expanded to.  Here they are
     * always real variables, and the less-equal-only logic is gated on max_d >= 0.
     */
    let mut start_column: c_int;
    let mut stop_column: c_int;

    /* Convert string lengths (in bytes) to lengths in characters */
    m = pg_mbstrlen_with_len(source, slen);
    n = pg_mbstrlen_with_len(target, tlen);

    /*
     * We can transform an empty s into t with n insertions, or a non-empty t
     * into an empty s with m deletions.
     */
    if m == 0 {
        return n * ins_c;
    }
    if n == 0 {
        return m * del_c;
    }

    /*
     * For security concerns, restrict excessive CPU+RAM usage. (This
     * implementation uses O(m) memory and has O(mn) complexity.)  If
     * "trusted" is true, caller is responsible for not making excessive
     * requests, typically by using a small max_d along with strings that are
     * bounded, though not necessarily to MAX_LEVENSHTEIN_STRLEN exactly.
     */
    if !trusted && (m > MAX_LEVENSHTEIN_STRLEN || n > MAX_LEVENSHTEIN_STRLEN) {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!(
                "levenshtein argument exceeds maximum length of {} characters",
                MAX_LEVENSHTEIN_STRLEN
            )
        );
    }

    /* Initialize start and stop columns. */
    start_column = 0;
    stop_column = m + 1;

    /*
     * If max_d >= 0, determine whether the bound is impossibly tight.  If so,
     * return max_d + 1 immediately.  Otherwise, determine whether it's tight
     * enough to limit the computation we must perform.  If so, figure out
     * initial stop column.
     */
    if max_d >= 0 {
        let min_theo_d: c_int; /* Theoretical minimum distance. */
        let max_theo_d: c_int; /* Theoretical maximum distance. */
        let net_inserts: c_int = n - m;

        min_theo_d = if net_inserts < 0 {
            -net_inserts * del_c
        } else {
            net_inserts * ins_c
        };
        if min_theo_d > max_d {
            return max_d + 1;
        }
        if ins_c + del_c < sub_c {
            sub_c = ins_c + del_c;
        }
        max_theo_d = min_theo_d + sub_c * Min(m, n);
        if max_d >= max_theo_d {
            max_d = -1;
        } else if ins_c + del_c > 0 {
            /*
             * Figure out how much of the first row of the notional matrix we
             * need to fill in.  If the string is growing, the theoretical
             * minimum distance already incorporates the cost of deleting the
             * number of characters necessary to make the two strings equal in
             * length.  Each additional deletion forces another insertion, so
             * the best-case total cost increases by ins_c + del_c. If the
             * string is shrinking, the minimum theoretical cost assumes no
             * excess deletions; that is, we're starting no further right than
             * column n - m.  If we do start further right, the best-case
             * total cost increases by ins_c + del_c for each move right.
             */
            let slack_d: c_int = max_d - min_theo_d;
            let best_column: c_int = if net_inserts < 0 { -net_inserts } else { 0 };

            stop_column = best_column + (slack_d / (ins_c + del_c)) + 1;
            if stop_column > m {
                stop_column = m + 1;
            }
        }
    }

    /*
     * In order to avoid calling pg_mblen_range() repeatedly on each character
     * in s, we cache all the lengths before starting the main loop -- but if
     * all the characters in both strings are single byte, then we skip this
     * and use a fast-path in the main loop.  If only one string contains
     * multi-byte characters, we still build the array, so that the fast-path
     * needn't deal with the case where the array hasn't been initialized.
     */
    if m != slen || n != tlen {
        let mut i: c_int;
        let mut cp: *const c_char = source;

        s_char_len = palloc(((m + 1) as usize) * core::mem::size_of::<c_int>()) as *mut c_int;
        i = 0;
        while i < m {
            // pg_mblen_range(cp, send): the remaining byte limit is send - cp.
            let cl = pg_mblen_with_len(cp, (send as isize - cp as isize) as c_int);
            *s_char_len.add(i as usize) = cl;
            cp = cp.add(cl as usize);
            i += 1;
        }
        *s_char_len.add(i as usize) = 0;
    }

    /* One more cell for initialization column and row. */
    m += 1;
    /* n += 1; -- n is consumed below via `n + 1` in the loop bound. */
    let n_plus_1: c_int = n + 1;

    /* Previous and current rows of notional array. */
    prev = palloc((2 * m) as usize * core::mem::size_of::<c_int>()) as *mut c_int;
    curr = prev.add(m as usize);

    /*
     * To transform the first i characters of s into the first 0 characters of
     * t, we must perform i deletions.
     */
    {
        let mut i: c_int = start_column;
        while i < stop_column {
            *prev.add(i as usize) = i * del_c;
            i += 1;
        }
    }

    /* Loop through rows of the notional array */
    y = target;
    let mut j: c_int = 1;
    while j < n_plus_1 {
        let temp: *mut c_int;
        let mut x: *const c_char = source;
        // pg_mblen_range(y, tend): n != tlen + 1 in the C means "n+1 != tlen+1",
        // i.e. some target char is multibyte; otherwise single-byte (length 1).
        let y_char_len: c_int = if n_plus_1 != tlen + 1 {
            pg_mblen_with_len(y, (tend as isize - y as isize) as c_int)
        } else {
            1
        };
        let mut i: c_int;

        if max_d >= 0 {
            /*
             * In the best case, values percolate down the diagonal unchanged, so
             * we must increment stop_column unless it's already on the right end
             * of the array.  The inner loop will read prev[stop_column], so we
             * have to initialize it even though it shouldn't affect the result.
             */
            if stop_column < m {
                *prev.add(stop_column as usize) = max_d + 1;
                stop_column += 1;
            }

            /*
             * The main loop fills in curr, but curr[0] needs a special case: to
             * transform the first 0 characters of s into the first j characters
             * of t, we must perform j insertions.  However, if start_column > 0,
             * this special case does not apply.
             */
            if start_column == 0 {
                *curr.add(0) = j * ins_c;
                i = 1;
            } else {
                i = start_column;
            }
        } else {
            *curr.add(0) = j * ins_c;
            i = 1;
        }

        /*
         * This inner loop is critical to performance, so we include a
         * fast-path to handle the (fairly common) case where no multibyte
         * characters are in the mix.  The fast-path is entitled to assume
         * that if s_char_len is not initialized then BOTH strings contain
         * only single-byte characters.
         */
        if !s_char_len.is_null() {
            while i < stop_column {
                let ins: c_int;
                let del: c_int;
                let sub: c_int;
                let x_char_len: c_int = *s_char_len.add((i - 1) as usize);

                /*
                 * Calculate costs for insertion, deletion, and substitution.
                 *
                 * When calculating cost for substitution, we compare the last
                 * character of each possibly-multibyte character first,
                 * because that's enough to rule out most mis-matches.  If we
                 * get past that test, then we compare the lengths and the
                 * remaining bytes.
                 */
                ins = *prev.add(i as usize) + ins_c;
                del = *curr.add((i - 1) as usize) + del_c;
                if *x.add((x_char_len - 1) as usize) == *y.add((y_char_len - 1) as usize)
                    && x_char_len == y_char_len
                    && (x_char_len == 1 || rest_of_char_same(x, y, x_char_len))
                {
                    sub = *prev.add((i - 1) as usize);
                } else {
                    sub = *prev.add((i - 1) as usize) + sub_c;
                }

                /* Take the one with minimum cost. */
                *curr.add(i as usize) = Min(ins, del);
                *curr.add(i as usize) = Min(*curr.add(i as usize), sub);

                /* Point to next character. */
                x = x.add(x_char_len as usize);

                i += 1;
            }
        } else {
            while i < stop_column {
                let ins: c_int;
                let del: c_int;
                let sub: c_int;

                /* Calculate costs for insertion, deletion, and substitution. */
                ins = *prev.add(i as usize) + ins_c;
                del = *curr.add((i - 1) as usize) + del_c;
                sub = *prev.add((i - 1) as usize) + (if *x == *y { 0 } else { sub_c });

                /* Take the one with minimum cost. */
                *curr.add(i as usize) = Min(ins, del);
                *curr.add(i as usize) = Min(*curr.add(i as usize), sub);

                /* Point to next character. */
                x = x.add(1);

                i += 1;
            }
        }

        /* Swap current row with previous row. */
        temp = curr;
        curr = prev;
        prev = temp;

        /* Point to next character. */
        y = y.add(y_char_len as usize);

        /*
         * This chunk of code represents a significant performance hit if used
         * in the case where there is no max_d bound.  This is probably not
         * because the max_d >= 0 test itself is expensive, but rather because
         * the possibility of needing to execute this code prevents tight
         * optimization of the loop as a whole.
         */
        if max_d >= 0 {
            /*
             * The "zero point" is the column of the current row where the
             * remaining portions of the strings are of equal length.  There
             * are (n - 1) characters in the target string, of which j have
             * been transformed.  There are (m - 1) characters in the source
             * string, so we want to find the value for zp where (n - 1) - j =
             * (m - 1) - zp.
             */
            let zp: c_int = j - (n_plus_1 - m);

            /* Check whether the stop column can slide left. */
            while stop_column > 0 {
                let ii: c_int = stop_column - 1;
                let net_inserts: c_int = ii - zp;

                if *prev.add(ii as usize)
                    + (if net_inserts > 0 {
                        net_inserts * ins_c
                    } else {
                        -net_inserts * del_c
                    })
                    <= max_d
                {
                    break;
                }
                stop_column -= 1;
            }

            /* Check whether the start column can slide right. */
            while start_column < stop_column {
                let net_inserts: c_int = start_column - zp;

                if *prev.add(start_column as usize)
                    + (if net_inserts > 0 {
                        net_inserts * ins_c
                    } else {
                        -net_inserts * del_c
                    })
                    <= max_d
                {
                    break;
                }

                /*
                 * We'll never again update these values, so we must make sure
                 * there's nothing here that could confuse any future
                 * iteration of the outer loop.
                 */
                *prev.add(start_column as usize) = max_d + 1;
                *curr.add(start_column as usize) = max_d + 1;
                if start_column != 0 {
                    source = source.add(if !s_char_len.is_null() {
                        *s_char_len.add((start_column - 1) as usize) as usize
                    } else {
                        1
                    });
                }
                start_column += 1;
            }

            /* If they cross, we're going to exceed the bound. */
            if start_column >= stop_column {
                return max_d + 1;
            }
        }

        j += 1;
    }

    /*
     * Because the final value was swapped from the previous row to the
     * current row, that's where we'll find it.
     */
    *prev.add((m - 1) as usize)
}

/*
 * ===========================================================================
 *  SQL-callable wrappers (contrib/fuzzystrmatch/fuzzystrmatch.c).
 *
 *  These obtain the byte data + byte length of the two text arguments and call
 *  the merged levenshtein_internal with the appropriate costs / bound.
 * ===========================================================================
 */

/// `PG_GETARG_TEXT_PP(n)` spelled inline per project convention (the detoaster
/// lives in crate::varatt; see CONVENTIONS).
///
/// # Safety
/// `fcinfo` holds at least `n+1` args, the n'th being a text Datum.
#[inline]
unsafe fn PG_GETARG_TEXT_PP(fcinfo: FunctionCallInfo, n: usize) -> *mut text {
    pg_detoast_datum_packed(
        crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, n)) as *mut c_void,
    ) as *mut text
}

/*
 * levenshtein_with_costs(text, text, int, int, int) -> int
 */
pub unsafe fn levenshtein_with_costs(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let dst: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let ins_c: c_int = PG_GETARG_INT32!(fcinfo, 2);
    let del_c: c_int = PG_GETARG_INT32!(fcinfo, 3);
    let sub_c: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let s_data: *const c_char;
    let t_data: *const c_char;
    let s_bytes: c_int;
    let t_bytes: c_int;

    /* Extract a pointer to the actual character data */
    s_data = VARDATA_ANY(src as *const c_char);
    t_data = VARDATA_ANY(dst as *const c_char);
    /* Determine length of each string in bytes */
    s_bytes = VARSIZE_ANY_EXHDR(src as *const c_char) as c_int;
    t_bytes = VARSIZE_ANY_EXHDR(dst as *const c_char) as c_int;

    // C: varstr_levenshtein(s_data, s_bytes, t_data, t_bytes, ins_c, del_c, sub_c, false)
    PG_RETURN_INT32!(levenshtein_internal(
        s_data, s_bytes, t_data, t_bytes, ins_c, del_c, sub_c, -1, false
    ));
}

/*
 * levenshtein(text, text) -> int  (default costs 1/1/1)
 */
pub unsafe fn levenshtein(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let dst: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let s_data: *const c_char;
    let t_data: *const c_char;
    let s_bytes: c_int;
    let t_bytes: c_int;

    /* Extract a pointer to the actual character data */
    s_data = VARDATA_ANY(src as *const c_char);
    t_data = VARDATA_ANY(dst as *const c_char);
    /* Determine length of each string in bytes */
    s_bytes = VARSIZE_ANY_EXHDR(src as *const c_char) as c_int;
    t_bytes = VARSIZE_ANY_EXHDR(dst as *const c_char) as c_int;

    // C: varstr_levenshtein(s_data, s_bytes, t_data, t_bytes, 1, 1, 1, false)
    PG_RETURN_INT32!(levenshtein_internal(
        s_data, s_bytes, t_data, t_bytes, 1, 1, 1, -1, false
    ));
}

/*
 * levenshtein_less_equal_with_costs(text, text, int, int, int, int) -> int
 */
pub unsafe fn levenshtein_less_equal_with_costs(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let dst: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let ins_c: c_int = PG_GETARG_INT32!(fcinfo, 2);
    let del_c: c_int = PG_GETARG_INT32!(fcinfo, 3);
    let sub_c: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let max_d: c_int = PG_GETARG_INT32!(fcinfo, 5);
    let s_data: *const c_char;
    let t_data: *const c_char;
    let s_bytes: c_int;
    let t_bytes: c_int;

    /* Extract a pointer to the actual character data */
    s_data = VARDATA_ANY(src as *const c_char);
    t_data = VARDATA_ANY(dst as *const c_char);
    /* Determine length of each string in bytes */
    s_bytes = VARSIZE_ANY_EXHDR(src as *const c_char) as c_int;
    t_bytes = VARSIZE_ANY_EXHDR(dst as *const c_char) as c_int;

    // C: varstr_levenshtein_less_equal(..., ins_c, del_c, sub_c, max_d, false)
    PG_RETURN_INT32!(levenshtein_internal(
        s_data, s_bytes, t_data, t_bytes, ins_c, del_c, sub_c, max_d, false
    ));
}

/*
 * levenshtein_less_equal(text, text, int) -> int  (default costs 1/1/1)
 */
pub unsafe fn levenshtein_less_equal(fcinfo: FunctionCallInfo) -> Datum {
    let src: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
    let dst: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let max_d: c_int = PG_GETARG_INT32!(fcinfo, 2);
    let s_data: *const c_char;
    let t_data: *const c_char;
    let s_bytes: c_int;
    let t_bytes: c_int;

    /* Extract a pointer to the actual character data */
    s_data = VARDATA_ANY(src as *const c_char);
    t_data = VARDATA_ANY(dst as *const c_char);
    /* Determine length of each string in bytes */
    s_bytes = VARSIZE_ANY_EXHDR(src as *const c_char) as c_int;
    t_bytes = VARSIZE_ANY_EXHDR(dst as *const c_char) as c_int;

    // C: varstr_levenshtein_less_equal(..., 1, 1, 1, max_d, false)
    PG_RETURN_INT32!(levenshtein_internal(
        s_data, s_bytes, t_data, t_bytes, 1, 1, 1, max_d, false
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetInt32, PointerGetDatum};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::adt::varlena::cstring_to_text;
    use crate::utils::fmgr::{
        DirectFunctionCall2Coll, DirectFunctionCall3Coll, DirectFunctionCall5Coll,
        DirectFunctionCall6Coll,
    };

    // Build a text Datum from a Rust &str (NUL-terminated cstring -> text).
    unsafe fn mk(s: &str) -> Datum {
        // s.as_bytes() has no trailing NUL, so go through a CString-like buffer.
        let mut v: std::vec::Vec<c_char> = s.bytes().map(|b| b as c_char).collect();
        v.push(0);
        let t = cstring_to_text(v.as_ptr());
        PointerGetDatum(t as *const c_void)
    }

    unsafe fn lev(a: &str, b: &str) -> i32 {
        DatumGetInt32(DirectFunctionCall2Coll(levenshtein, InvalidOid, mk(a), mk(b)))
    }

    #[test]
    fn levenshtein_basic_distances() {
        unsafe {
            assert_eq!(lev("", ""), 0);
            assert_eq!(lev("", "abc"), 3); // 3 insertions
            assert_eq!(lev("abc", ""), 3); // 3 deletions
            assert_eq!(lev("abc", "abc"), 0); // identical
            assert_eq!(lev("kitten", "sitting"), 3); // classic example
            assert_eq!(lev("GUMBO", "GAMBOL"), 2);
            assert_eq!(lev("sunday", "saturday"), 3);
            // single substitution / insertion / deletion
            assert_eq!(lev("cat", "bat"), 1);
            assert_eq!(lev("cat", "cats"), 1);
            assert_eq!(lev("cats", "cat"), 1);
        }
    }

    #[test]
    fn levenshtein_with_costs_weighted() {
        unsafe {
            use crate::postgres::Int32GetDatum;
            // ins=10, del=1, sub=1: "ab" -> "abc" needs 1 insertion = 10.
            let d = DirectFunctionCall5Coll(
                levenshtein_with_costs,
                InvalidOid,
                mk("ab"),
                mk("abc"),
                Int32GetDatum(10),
                Int32GetDatum(1),
                Int32GetDatum(1),
            );
            assert_eq!(DatumGetInt32(d), 10);

            // ins=1, del=10, sub=1: "abc" -> "ab" needs 1 deletion = 10.
            let d2 = DirectFunctionCall5Coll(
                levenshtein_with_costs,
                InvalidOid,
                mk("abc"),
                mk("ab"),
                Int32GetDatum(1),
                Int32GetDatum(10),
                Int32GetDatum(1),
            );
            assert_eq!(DatumGetInt32(d2), 10);

            // Even without the explicit less_equal sub-cost cap, the DP itself
            // finds the cheaper delete+insert path: "a" -> "b" with sub=100,
            // ins=del=1 costs 1+1 = 2 (delete 'a', insert 'b'), not 100.
            let d3 = DirectFunctionCall5Coll(
                levenshtein_with_costs,
                InvalidOid,
                mk("a"),
                mk("b"),
                Int32GetDatum(1),
                Int32GetDatum(1),
                Int32GetDatum(100),
            );
            assert_eq!(DatumGetInt32(d3), 2);
        }
    }

    #[test]
    fn levenshtein_less_equal_bound() {
        unsafe {
            use crate::postgres::Int32GetDatum;
            // Distance("kitten","sitting") == 3.  With max_d = 2 the bound is
            // exceeded, so the function returns max_d + 1 == 3 (coincidentally),
            // but with a longer mismatch the cap is clearer.
            let within = DirectFunctionCall3Coll(
                levenshtein_less_equal,
                InvalidOid,
                mk("kitten"),
                mk("sitting"),
                Int32GetDatum(3),
            );
            assert_eq!(DatumGetInt32(within), 3); // exact, within bound

            // Far apart strings with a tight bound return max_d + 1.
            let exceeded = DirectFunctionCall3Coll(
                levenshtein_less_equal,
                InvalidOid,
                mk("aaaaaaaa"),
                mk("bbbbbbbb"),
                Int32GetDatum(2),
            );
            assert_eq!(DatumGetInt32(exceeded), 3); // 2 + 1

            // less_equal_with_costs threading all six args; exact distance 1
            // is within bound 5.
            let lew = DirectFunctionCall6Coll(
                levenshtein_less_equal_with_costs,
                InvalidOid,
                mk("cat"),
                mk("cot"),
                Int32GetDatum(1),
                Int32GetDatum(1),
                Int32GetDatum(1),
                Int32GetDatum(5),
            );
            assert_eq!(DatumGetInt32(lew), 1);
        }
    }

    #[test]
    fn levenshtein_multibyte_utf8() {
        unsafe {
            // UTF8 chars: "café" vs "cafe" differ by one char (é vs e) = 1 sub.
            assert_eq!(lev("caf\u{00e9}", "cafe"), 1);
            // two euro signs vs one euro sign: one insertion of a 3-byte char.
            assert_eq!(lev("\u{20ac}", "\u{20ac}\u{20ac}"), 1);
            // identical multibyte strings -> 0.
            assert_eq!(lev("\u{00e9}\u{20ac}", "\u{00e9}\u{20ac}"), 0);
        }
    }

    #[test]
    #[should_panic]
    fn levenshtein_rejects_overlong_untrusted() {
        unsafe {
            // A string longer than MAX_LEVENSHTEIN_STRLEN (255) chars with the
            // untrusted SQL entry point must ereport(ERROR).
            let long_a = "a".repeat(300);
            let long_b = "b".repeat(300);
            let _ = lev(&long_a, &long_b);
        }
    }
}
