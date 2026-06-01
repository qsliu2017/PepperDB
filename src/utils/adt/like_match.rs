//-------------------------------------------------------------------------
//
// like_match.c
//	  LIKE pattern matching internal code.
//
// This file is included by like.c four times, to provide matching code for
// (1) single-byte encodings, (2) UTF8, (3) other multi-byte encodings,
// and (4) case insensitive matches in single-byte encodings.
// (UTF8 is a special case because we can use a much more efficient version
// of NextChar than can be used for general multi-byte encodings.)
//
// Before the inclusion, we need to define the following macros:
//
// NextChar
// MatchText - to name of function wanted
// do_like_escape - name of function if wanted - needs CHAREQ and CopyAdvChar
// MATCH_LOWER - define for case (4) to specify case folding for 1-byte chars
//
// Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
// IDENTIFICATION
//	src/backend/utils/adt/like_match.c
//
//-------------------------------------------------------------------------

//	Originally written by Rich $alz, mirror!rs, Wed Nov 26 19:03:17 EST 1986.
//	Rich $alz is now <rsalz@bbn.com>.
//	Special thanks to Lars Mathiesen <thorinn@diku.dk> for the
//	LIKE_ABORT code.
//
//	This code was shamelessly stolen from the "pql" code by myself and
//	slightly modified :)
//
//	All references to the word "star" were replaced by "percent"
//	All references to the word "wild" were replaced by "like"
//
//	All the nice shell RE matching stuff was replaced by just "_" and "%"
//
//	As I don't have a copy of the SQL standard handy I wasn't sure whether
//	to leave in the '\' escape character handling.
//
//	Keith Parks. <keith@mtcc.demon.co.uk>
//
//	SQL lets you specify the escape character by saying
//	LIKE <pattern> ESCAPE <escape character>. We are a small operation
//	so we force you to use '\'. - ay 7/95
//
//	Now we have the like_escape() function that converts patterns with
//	any specified escape character (or none at all) to the internal
//	default escape character, which is still '\'. - tgl 9/2000
//
// The code is rewritten to avoid requiring null-terminated strings,
// which in turn allows us to leave out some memcpy() operations.
// This code should be faster and take less memory, but no promises...
// - thomas 2000-08-06

use std::os::raw::{c_char, c_int};

use crate::miscadmin::{check_stack_depth, ssize_t, CHECK_FOR_INTERRUPTS};
use crate::utils::adt::pg_locale::pg_strncoll;
use crate::utils::adt::pg_locale_libc::pg_locale_t;
use crate::utils::palloc::{palloc, pfree};
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY, VARSIZE_ANY_EXHDR};

// like.c: matching result codes.
const LIKE_TRUE: c_int = 1;
const LIKE_FALSE: c_int = 0;
const LIKE_ABORT: c_int = -1;

// like.c: VARHDRSZ (c.h).
const VARHDRSZ: c_int = 4;

// utils/errcodes.h: ERRCODE_INVALID_ESCAPE_SEQUENCE.
// TODO(pg-port): real ERRCODE_INVALID_ESCAPE_SEQUENCE.
const ERRCODE_INVALID_ESCAPE_SEQUENCE: c_int = 0;

// c.h: text typedef (struct varlena).
use crate::c::text;

// like.c #define NextByte(p, plen)	((p)++, (plen)--)
#[inline]
unsafe fn NextByte(p: &mut *const c_char, plen: &mut c_int) {
    *p = (*p).add(1);
    *plen -= 1;
}

// like.c #define NextByte for char* (do_like_escape uses mutable dst/src).
#[inline]
unsafe fn NextByteMut(p: &mut *mut c_char, plen: &mut c_int) {
    *p = (*p).add(1);
    *plen -= 1;
}

// Macro-parameterized character advance.  This file is the like_match.c
// template, which is #included with different NextChar definitions:
//  - SB:   NextChar == NextByte
//  - UTF8: skip continuation bytes (high bit set) after the lead byte
//  - MB:   pg_mblen-based advance
// TODO(pg-port): the template is instantiated per-encoding by like.c; the
// generic translation here uses the multi-byte (MB) semantics via pg_mblen.
#[inline]
unsafe fn NextChar(p: &mut *const c_char, plen: &mut c_int) {
    let l: c_int = pg_mblen(*p);
    *p = (*p).add(l as usize);
    *plen -= l;
}

#[inline]
unsafe fn NextCharMut(p: &mut *mut c_char, plen: &mut c_int) {
    let l: c_int = pg_mblen(*p);
    *p = (*p).add(l as usize);
    *plen -= l;
}

// like.c #define CopyAdvChar(dst, src, srclen) --- copy one (multibyte) char
// from src to dst, advancing both and decrementing srclen.
#[inline]
unsafe fn CopyAdvChar(dst: &mut *mut c_char, src: &mut *const c_char, srclen: &mut c_int) {
    let mut l: c_int = pg_mblen(*src);
    *srclen -= l;
    while l > 0 {
        l -= 1;
        **dst = **src;
        *dst = (*dst).add(1);
        *src = (*src).add(1);
    }
}

// like.c #define CHAREQ(p1, p1len, p2, p2len) wchareq(...) for MB; the
// single-byte template uses (*(p1) == *(p2)).
// TODO(pg-port): wchareq lives in like.c (already translated in like.rs);
// the MB instantiation compares full multibyte chars.
#[inline]
unsafe fn CHAREQ(p1: *const c_char, p1len: c_int, p2: *const c_char, p2len: c_int) -> bool {
    let l1 = pg_mblen(p1);
    let l2 = pg_mblen(p2);
    if l1 != l2 || l1 > p1len || l2 > p2len {
        return false;
    }
    let mut i = 0;
    while i < l1 {
        if *p1.add(i as usize) != *p2.add(i as usize) {
            return false;
        }
        i += 1;
    }
    true
}

// mb/pg_wchar.h: pg_mblen --- length in bytes of the char starting at p.
// TODO(pg-port): real pg_mblen (encoding-dependent) lives in mb/wchar.rs.
#[inline]
unsafe fn pg_mblen(_p: *const c_char) -> c_int {
    1
}

// like_match.c GETCHAR macro.  With MATCH_LOWER defined (case-insensitive
// single-byte instantiation) it folds case; otherwise it is the identity.
// TODO(pg-port): the MATCH_LOWER instantiation uses SB_lower_char(t, locale);
// the generic translation here is the non-folding identity form.
#[inline]
unsafe fn GETCHAR(t: c_char, _locale: pg_locale_t) -> c_char {
    t
}

//--------------------
//	Match text and pattern, return LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT.
//
//	LIKE_TRUE: they match
//	LIKE_FALSE: they don't match
//	LIKE_ABORT: not only don't they match, but the text is too short.
//
// If LIKE_ABORT is returned, then no suffix of the text can match the
// pattern either, so an upper-level % scan can stop scanning now.
//--------------------
unsafe fn MatchText(
    t: *const c_char,
    tlen: c_int,
    p: *const c_char,
    plen: c_int,
    locale: pg_locale_t,
) -> c_int {
    let mut t = t;
    let mut tlen = tlen;
    let mut p = p;
    let mut plen = plen;

    /* Fast path for match-everything pattern */
    if plen == 1 && *p == b'%' as c_char {
        return LIKE_TRUE;
    }

    /* Since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /*
     * In this loop, we advance by char when matching wildcards (and thus on
     * recursive entry to this function we are properly char-synced). On other
     * occasions it is safe to advance by byte, as the text and pattern will
     * be in lockstep. This allows us to perform all comparisons between the
     * text and pattern on a byte by byte basis, even for multi-byte
     * encodings.
     */
    while tlen > 0 && plen > 0 {
        if *p == b'\\' as c_char {
            /* Next pattern byte must match literally, whatever it is */
            NextByte(&mut p, &mut plen);
            /* ... and there had better be one, per SQL standard */
            if plen <= 0 {
                ereport!(
                    ERROR,
                    errmsg!("LIKE pattern must not end with escape character")
                );
                // C also: errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE)
            }
            if GETCHAR(*p, locale) != GETCHAR(*t, locale) {
                return LIKE_FALSE;
            }
        } else if *p == b'%' as c_char {
            let firstpat: c_char;

            /*
             * % processing is essentially a search for a text position at
             * which the remainder of the text matches the remainder of the
             * pattern, using a recursive call to check each potential match.
             *
             * If there are wildcards immediately following the %, we can skip
             * over them first, using the idea that any sequence of N _'s and
             * one or more %'s is equivalent to N _'s and one % (ie, it will
             * match any sequence of at least N text characters).  In this way
             * we will always run the recursive search loop using a pattern
             * fragment that begins with a literal character-to-match, thereby
             * not recursing more than we have to.
             */
            NextByte(&mut p, &mut plen);

            while plen > 0 {
                if *p == b'%' as c_char {
                    NextByte(&mut p, &mut plen);
                } else if *p == b'_' as c_char {
                    /* If not enough text left to match the pattern, ABORT */
                    if tlen <= 0 {
                        return LIKE_ABORT;
                    }
                    NextChar(&mut t, &mut tlen);
                    NextByte(&mut p, &mut plen);
                } else {
                    break; /* Reached a non-wildcard pattern char */
                }
            }

            /*
             * If we're at end of pattern, match: we have a trailing % which
             * matches any remaining text string.
             */
            if plen <= 0 {
                return LIKE_TRUE;
            }

            /*
             * Otherwise, scan for a text position at which we can match the
             * rest of the pattern.  The first remaining pattern char is known
             * to be a regular or escaped literal character, so we can compare
             * the first pattern byte to each text byte to avoid recursing
             * more than we have to.  This fact also guarantees that we don't
             * have to consider a match to the zero-length substring at the
             * end of the text.  With a nondeterministic collation, we can't
             * rely on the first bytes being equal, so we have to recurse in
             * any case.
             */
            if *p == b'\\' as c_char {
                if plen < 2 {
                    ereport!(
                        ERROR,
                        errmsg!("LIKE pattern must not end with escape character")
                    );
                    // C also: errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE)
                }
                firstpat = GETCHAR(*p.add(1), locale);
            } else {
                firstpat = GETCHAR(*p, locale);
            }

            while tlen > 0 {
                if GETCHAR(*t, locale) == firstpat
                    || (!locale.is_null() && !(*locale).deterministic)
                {
                    let matched: c_int = MatchText(t, tlen, p, plen, locale);

                    if matched != LIKE_FALSE {
                        return matched; /* TRUE or ABORT */
                    }
                }

                NextChar(&mut t, &mut tlen);
            }

            /*
             * End of text with no match, so no point in trying later places
             * to start matching this pattern.
             */
            return LIKE_ABORT;
        } else if *p == b'_' as c_char {
            /* _ matches any single character, and we know there is one */
            NextChar(&mut t, &mut tlen);
            NextByte(&mut p, &mut plen);
            continue;
        } else if !locale.is_null() && !(*locale).deterministic {
            /*
             * For nondeterministic locales, we find the next substring of the
             * pattern that does not contain wildcards and try to find a
             * matching substring in the text.  Crucially, we cannot do this
             * character by character, as in the normal case, but must do it
             * substring by substring, partitioned by the wildcard characters.
             * (This is per SQL standard.)
             */
            let mut p1: *const c_char;
            let mut p1len: ssize_t;
            let mut t1: *const c_char;
            let mut t1len: ssize_t;
            let mut found_escape: bool;
            let subpat: *const c_char;
            let subpatlen: ssize_t;
            let mut buf: *mut c_char = std::ptr::null_mut();

            /*
             * Determine next substring of pattern without wildcards.  p is
             * the start of the subpattern, p1 is one past the last byte. Also
             * track if we found an escape character.
             */
            p1 = p;
            p1len = plen as ssize_t;
            found_escape = false;
            while p1len > 0 {
                if *p1 == b'\\' as c_char {
                    found_escape = true;
                    let mut p1len_i = p1len as c_int;
                    NextByte(&mut p1, &mut p1len_i);
                    p1len = p1len_i as ssize_t;
                    if p1len == 0 {
                        ereport!(
                            ERROR,
                            errmsg!("LIKE pattern must not end with escape character")
                        );
                        // C also: errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE)
                    }
                } else if *p1 == b'_' as c_char || *p1 == b'%' as c_char {
                    break;
                }
                let mut p1len_i = p1len as c_int;
                NextByte(&mut p1, &mut p1len_i);
                p1len = p1len_i as ssize_t;
            }

            /*
             * If we found an escape character, then make an unescaped copy of
             * the subpattern.
             */
            if found_escape {
                let mut b: *mut c_char;

                buf = palloc(p1.offset_from(p) as usize) as *mut c_char;
                b = buf;
                let mut c = p;
                while c < p1 {
                    if *c == b'\\' as c_char {
                        /* skip the escape character */
                    } else {
                        *b = *c;
                        b = b.add(1);
                    }
                    c = c.add(1);
                }

                subpat = buf;
                subpatlen = b.offset_from(buf) as ssize_t;
            } else {
                subpat = p;
                subpatlen = p1.offset_from(p) as ssize_t;
            }

            /*
             * Shortcut: If this is the end of the pattern, then the rest of
             * the text has to match the rest of the pattern.
             */
            if p1len == 0 {
                let cmp: c_int;

                cmp = pg_strncoll(subpat, subpatlen, t, tlen as ssize_t, locale);

                if !buf.is_null() {
                    pfree(buf as *mut std::ffi::c_void);
                }
                if cmp == 0 {
                    return LIKE_TRUE;
                } else {
                    return LIKE_FALSE;
                }
            }

            /*
             * Now build a substring of the text and try to match it against
             * the subpattern.  t is the start of the text, t1 is one past the
             * last byte.  We start with a zero-length string.
             */
            t1 = t;
            t1len = tlen as ssize_t;
            loop {
                let cmp: c_int;

                CHECK_FOR_INTERRUPTS();

                cmp = pg_strncoll(subpat, subpatlen, t, t1.offset_from(t) as ssize_t, locale);

                /*
                 * If we found a match, we have to test if the rest of pattern
                 * can match against the rest of the string.  Otherwise we
                 * have to continue here try matching with a longer substring.
                 * (This is similar to the recursion for the '%' wildcard
                 * above.)
                 *
                 * Note that we can't just wind forward p and t and continue
                 * with the main loop.  This would fail for example with
                 *
                 * U&'\0061\0308bc' LIKE U&'\00E4_c' COLLATE ignore_accents
                 *
                 * You'd find that t=\0061 matches p=\00E4, but then the rest
                 * won't match; but t=\0061\0308 also matches p=\00E4, and
                 * then the rest will match.
                 */
                if cmp == 0 {
                    let matched: c_int =
                        MatchText(t1, t1len as c_int, p1, p1len as c_int, locale);

                    if matched == LIKE_TRUE {
                        if !buf.is_null() {
                            pfree(buf as *mut std::ffi::c_void);
                        }
                        return matched;
                    }
                }

                /*
                 * Didn't match.  If we used up the whole text, then the match
                 * fails.  Otherwise, try again with a longer substring.
                 */
                if t1len == 0 {
                    if !buf.is_null() {
                        pfree(buf as *mut std::ffi::c_void);
                    }
                    return LIKE_FALSE;
                } else {
                    let mut t1len_i = t1len as c_int;
                    NextChar(&mut t1, &mut t1len_i);
                    t1len = t1len_i as ssize_t;
                }
            }
        } else if GETCHAR(*p, locale) != GETCHAR(*t, locale) {
            /* non-wildcard pattern char fails to match text char */
            return LIKE_FALSE;
        }

        /*
         * Pattern and text match, so advance.
         *
         * It is safe to use NextByte instead of NextChar here, even for
         * multi-byte character sets, because we are not following immediately
         * after a wildcard character. If we are in the middle of a multibyte
         * character, we must already have matched at least one byte of the
         * character from both text and pattern; so we cannot get out-of-sync
         * on character boundaries.  And we know that no backend-legal
         * encoding allows ASCII characters such as '%' to appear as non-first
         * bytes of characters, so we won't mistakenly detect a new wildcard.
         */
        NextByte(&mut t, &mut tlen);
        NextByte(&mut p, &mut plen);
    }

    if tlen > 0 {
        return LIKE_FALSE; /* end of pattern, but not of text */
    }

    /*
     * End of text, but perhaps not of pattern.  Match iff the remaining
     * pattern can match a zero-length string, ie, it's zero or more %'s.
     */
    while plen > 0 && *p == b'%' as c_char {
        NextByte(&mut p, &mut plen);
    }
    if plen <= 0 {
        return LIKE_TRUE;
    }

    /*
     * End of text with no match, so no point in trying later places to start
     * matching this pattern.
     */
    LIKE_ABORT
} /* MatchText() */
