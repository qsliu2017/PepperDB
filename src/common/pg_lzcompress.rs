//! Translated from PostgreSQL 18.3:
//!   - src/include/common/pg_lzcompress.h
//!   - src/common/pg_lzcompress.c
//!
//! This is an implementation of LZ compression for PostgreSQL.
//! It uses a simple history table and generates 2-3 byte tags
//! capable of backward copy information for 3-273 bytes with
//! a max offset of 4095.
//!
//! See the original C header comment in pg_lzcompress.c for the full
//! description of the algorithm and on-disk data format.
//!
//! Copyright (c) 1999-2025, PostgreSQL Global Development Group

use crate::prelude::*;
use core::ptr::addr_of_mut;

// INT_MAX from <limits.h>
const INT_MAX: int32 = i32::MAX;

/* ----------
 * PGLZ_MAX_OUTPUT -
 *
 *		Macro to compute the buffer size required by pglz_compress().
 *		We allow 4 bytes for overrun before detecting compression failure.
 * ----------
 */
#[allow(non_snake_case)]
#[inline]
pub fn PGLZ_MAX_OUTPUT(_dlen: int32) -> int32 {
    _dlen + 4
}

/* ----------
 * PGLZ_Strategy -
 *
 *		Some values that control the compression algorithm.
 *
 *		min_input_size		Minimum input data size to consider compression.
 *
 *		max_input_size		Maximum input data size to consider compression.
 *
 *		min_comp_rate		Minimum compression rate (0-99%) to require.
 *							Regardless of min_comp_rate, the output must be
 *							smaller than the input, else we don't store
 *							compressed.
 *
 *		first_success_by	Abandon compression if we find no compressible
 *							data within the first this-many bytes.
 *
 *		match_size_good		The initial GOOD match size when starting history
 *							lookup. When looking up the history to find a
 *							match that could be expressed as a tag, the
 *							algorithm does not always walk back entirely.
 *							A good match fast is usually better than the
 *							best possible one very late. For each iteration
 *							in the lookup, this value is lowered so the
 *							longer the lookup takes, the smaller matches
 *							are considered good.
 *
 *		match_size_drop		The percentage by which match_size_good is lowered
 *							after each history check. Allowed values are
 *							0 (no change until end) to 100 (only check
 *							latest history entry at all).
 * ----------
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PGLZ_Strategy {
    pub min_input_size: int32,
    pub max_input_size: int32,
    pub min_comp_rate: int32,
    pub first_success_by: int32,
    pub match_size_good: int32,
    pub match_size_drop: int32,
}

/* ----------
 * Local definitions
 * ----------
 */
const PGLZ_MAX_HISTORY_LISTS: usize = 8192; /* must be power of 2 */
const PGLZ_HISTORY_SIZE: usize = 4096;
const PGLZ_MAX_MATCH: int32 = 273;

/* ----------
 * PGLZ_HistEntry -
 *
 *		Linked list for the backward history lookup
 *
 * All the entries sharing a hash key are linked in a doubly linked list.
 * This makes it easy to remove an entry when it's time to recycle it
 * (because it's more than 4K positions old).
 * ----------
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PGLZ_HistEntry {
    pub next: *mut PGLZ_HistEntry, /* links for my hash key's list */
    pub prev: *mut PGLZ_HistEntry,
    pub hindex: c_int, /* my current hash key */
    pub pos: *const c_char, /* my input position */
}

/* ----------
 * The provided standard strategies
 * ----------
 */
static strategy_default_data: PGLZ_Strategy = PGLZ_Strategy {
    min_input_size: 32, /* Data chunks less than 32 bytes are not
                         * compressed */
    max_input_size: INT_MAX, /* No upper limit on what we'll try to
                              * compress */
    min_comp_rate: 25, /* Require 25% compression rate, or not worth
                        * it */
    first_success_by: 1024, /* Give up if no compression in the first 1KB */
    match_size_good: 128, /* Stop history lookup if a match of 128 bytes
                           * is found */
    match_size_drop: 10, /* Lower good match size by 10% at every loop
                          * iteration */
};

/// PGLZ_strategy_default - Recommended default strategy for TOAST.
pub static PGLZ_strategy_default: &PGLZ_Strategy = &strategy_default_data;

static strategy_always_data: PGLZ_Strategy = PGLZ_Strategy {
    min_input_size: 0, /* Chunks of any size are compressed */
    max_input_size: INT_MAX,
    min_comp_rate: 0, /* It's enough to save one single byte */
    first_success_by: INT_MAX, /* Never give up early */
    match_size_good: 128, /* Stop history lookup if a match of 128 bytes
                           * is found */
    match_size_drop: 6, /* Look harder for a good match */
};

/// PGLZ_strategy_always - Try to compress inputs of any length.
/// Fallback to uncompressed storage only if output would be larger than input.
pub static PGLZ_strategy_always: &PGLZ_Strategy = &strategy_always_data;

/* ----------
 * Statically allocated work arrays for history
 * ----------
 *
 * These are global mutable scratch buffers, exactly like the C statics.
 * pglz_compress is not reentrant in PostgreSQL either, so we mirror that by
 * using a private global cell. Access is gated behind unsafe in the
 * compression routine.
 */
struct HistWorkArrays {
    hist_start: [int16; PGLZ_MAX_HISTORY_LISTS],
    hist_entries: [PGLZ_HistEntry; PGLZ_HISTORY_SIZE + 1],
}

// TODO(pg-port): The C code uses plain `static` (mutable) scratch arrays which
// rely on PostgreSQL's single-threaded backend model for safety. We mirror that
// with a mutable static; access is confined to pglz_compress, matching the C
// non-reentrancy contract.
static mut HIST_WORK: HistWorkArrays = HistWorkArrays {
    hist_start: [0; PGLZ_MAX_HISTORY_LISTS],
    hist_entries: [PGLZ_HistEntry {
        next: core::ptr::null_mut(),
        prev: core::ptr::null_mut(),
        hindex: 0,
        pos: core::ptr::null(),
    }; PGLZ_HISTORY_SIZE + 1],
};

/*
 * Element 0 in hist_entries is unused, and means 'invalid'. Likewise,
 * INVALID_ENTRY_PTR in next/prev pointers mean 'invalid'.
 */
const INVALID_ENTRY: usize = 0;

#[inline]
unsafe fn invalid_entry_ptr() -> *mut PGLZ_HistEntry {
    addr_of_mut!(HIST_WORK.hist_entries[INVALID_ENTRY])
}

/* ----------
 * pglz_hist_idx -
 *
 *		Computes the history table slot for the lookup by the next 4
 *		characters in the input.
 *
 * NB: because we use the next 4 characters, we are not guaranteed to
 * find 3-character matches; they very possibly will be in the wrong
 * hash list.  This seems an acceptable tradeoff for spreading out the
 * hash keys more.
 * ----------
 */
#[inline]
unsafe fn pglz_hist_idx(_s: *const c_char, _e: *const c_char, _mask: c_int) -> c_int {
    // ((((_e) - (_s)) < 4) ? (int) (_s)[0] :
    //  (((_s)[0] << 6) ^ ((_s)[1] << 4) ^ ((_s)[2] << 2) ^ (_s)[3])) & (_mask)
    //
    // The bytes are loaded as `char` (signed on most C platforms but the value
    // is just used as an int and masked at the end). We replicate the C int
    // arithmetic with wrapping ops since shifts/xors of byte-derived ints can
    // produce values that get masked down anyway.
    let raw = if (_e as isize - _s as isize) < 4 {
        *_s as c_int
    } else {
        ((*_s as c_int) << 6)
            ^ ((*_s.add(1) as c_int) << 4)
            ^ ((*_s.add(2) as c_int) << 2)
            ^ (*_s.add(3) as c_int)
    };
    raw & _mask
}

/* ----------
 * pglz_hist_add -
 *
 *		Adds a new entry to the history table.
 *
 * If _recycle is true, then we are recycling a previously used entry,
 * and must first delink it from its old hashcode's linked list.
 *
 * NOTE: beware of multiple evaluations of macro's arguments, and note that
 * _hn and _recycle are modified in the macro.
 * ----------
 *
 * Translated as an inline function. _hn (hist_next) and _recycle
 * (hist_recycle) are passed by mutable reference to mirror the macro's
 * in-place modification. _hs and _he are pointers into the global work arrays.
 */
#[inline]
unsafe fn pglz_hist_add(
    _hs: *mut int16,
    _he: *mut PGLZ_HistEntry,
    _hn: &mut c_int,
    _recycle: &mut bool,
    _s: *const c_char,
    _e: *const c_char,
    _mask: c_int,
) {
    let __hindex: c_int = pglz_hist_idx(_s, _e, _mask);
    let __myhsp: *mut int16 = _hs.add(__hindex as usize);
    let __myhe: *mut PGLZ_HistEntry = _he.add(*_hn as usize);

    if *_recycle {
        if (*__myhe).prev.is_null() {
            // (_hs)[__myhe->hindex] = __myhe->next - (_he);
            let diff = ((*__myhe).next as isize - _he as isize)
                / core::mem::size_of::<PGLZ_HistEntry>() as isize;
            *_hs.add((*__myhe).hindex as usize) = diff as int16;
        } else {
            (*(*__myhe).prev).next = (*__myhe).next;
        }
        if !(*__myhe).next.is_null() {
            (*(*__myhe).next).prev = (*__myhe).prev;
        }
    }

    (*__myhe).next = _he.add(*__myhsp as usize);
    (*__myhe).prev = core::ptr::null_mut();
    (*__myhe).hindex = __hindex;
    (*__myhe).pos = _s;
    /* If there was an existing entry in this hash slot, link */
    /* this new entry to it. However, the 0th entry in the */
    /* entries table is unused, so we can freely scribble on it. */
    /* So don't bother checking if the slot was used - we'll */
    /* scribble on the unused entry if it was not, but that's */
    /* harmless. Avoiding the branch in this critical path */
    /* speeds this up a little bit. */
    /* if (*__myhsp != INVALID_ENTRY) */
    (*_he.add(*__myhsp as usize)).prev = __myhe;
    *__myhsp = *_hn as int16;
    *_hn += 1;
    if *_hn >= (PGLZ_HISTORY_SIZE as c_int) + 1 {
        *_hn = 1;
        *_recycle = true;
    }
}

/* ----------
 * pglz_out_ctrl -
 *
 *		Outputs the last and allocates a new control byte if needed.
 * ----------
 *
 * Translated inline. __ctrlp, __ctrlb, __ctrl and __buf are passed by mutable
 * reference; __ctrlp points into the output buffer.
 */
#[inline]
unsafe fn pglz_out_ctrl(
    __ctrlp: &mut *mut c_uchar,
    __ctrlb: &mut c_uchar,
    __ctrl: &mut c_int,
    __buf: &mut *mut c_uchar,
) {
    if (*__ctrl & 0xff) == 0 {
        **__ctrlp = *__ctrlb;
        *__ctrlp = *__buf;
        *__buf = (*__buf).add(1);
        *__ctrlb = 0;
        *__ctrl = 1;
    }
}

/* ----------
 * pglz_out_literal -
 *
 *		Outputs a literal byte to the destination buffer including the
 *		appropriate control bit.
 * ----------
 */
#[inline]
unsafe fn pglz_out_literal(
    _ctrlp: &mut *mut c_uchar,
    _ctrlb: &mut c_uchar,
    _ctrl: &mut c_int,
    _buf: &mut *mut c_uchar,
    _byte: c_uchar,
) {
    pglz_out_ctrl(_ctrlp, _ctrlb, _ctrl, _buf);
    **_buf = _byte;
    *_buf = (*_buf).add(1);
    *_ctrl <<= 1;
}

/* ----------
 * pglz_out_tag -
 *
 *		Outputs a backward reference tag of 2-4 bytes (depending on
 *		offset and length) to the destination buffer including the
 *		appropriate control bit.
 * ----------
 */
#[inline]
unsafe fn pglz_out_tag(
    _ctrlp: &mut *mut c_uchar,
    _ctrlb: &mut c_uchar,
    _ctrl: &mut c_int,
    _buf: &mut *mut c_uchar,
    _len: int32,
    _off: int32,
) {
    pglz_out_ctrl(_ctrlp, _ctrlb, _ctrl, _buf);
    *_ctrlb |= *_ctrl as c_uchar;
    *_ctrl <<= 1;
    if _len > 17 {
        *(*_buf).add(0) = (((_off & 0xf00) >> 4) | 0x0f) as c_uchar;
        *(*_buf).add(1) = (_off & 0xff) as c_uchar;
        *(*_buf).add(2) = (_len - 18) as c_uchar;
        *_buf = (*_buf).add(3);
    } else {
        *(*_buf).add(0) = (((_off & 0xf00) >> 4) | (_len - 3)) as c_uchar;
        *(*_buf).add(1) = (_off & 0xff) as c_uchar;
        *_buf = (*_buf).add(2);
    }
}

/* ----------
 * pglz_find_match -
 *
 *		Lookup the history table if the actual input stream matches
 *		another sequence of characters, starting somewhere earlier
 *		in the input buffer.
 * ----------
 */
#[inline]
unsafe fn pglz_find_match(
    hstart: *mut int16,
    input: *const c_char,
    end: *const c_char,
    lenp: *mut c_int,
    offp: *mut c_int,
    mut good_match: c_int,
    good_drop: c_int,
    mask: c_int,
) -> c_int {
    let mut hent: *mut PGLZ_HistEntry;
    let hentno: int16;
    let mut len: int32 = 0;
    let mut off: int32 = 0;

    /*
     * Traverse the linked history list until a good enough match is found.
     */
    hentno = *hstart.add(pglz_hist_idx(input, end, mask) as usize);
    hent = addr_of_mut!(HIST_WORK.hist_entries[hentno as usize]);
    while hent != invalid_entry_ptr() {
        let mut ip: *const c_char = input;
        let mut hp: *const c_char = (*hent).pos;
        let thisoff: int32;
        let mut thislen: int32;

        /*
         * Stop if the offset does not fit into our tag anymore.
         */
        thisoff = (ip as isize - hp as isize) as int32;
        if thisoff >= 0x0fff {
            break;
        }

        /*
         * Determine length of match. A better match must be larger than the
         * best so far. And if we already have a match of 16 or more bytes,
         * it's worth the call overhead to use memcmp() to check if this match
         * is equal for the same size. After that we must fallback to
         * character by character comparison to know the exact position where
         * the diff occurred.
         */
        thislen = 0;
        if len >= 16 {
            if libc_memcmp(ip, hp, len as usize) == 0 {
                thislen = len;
                ip = ip.add(len as usize);
                hp = hp.add(len as usize);
                while ip < end && *ip == *hp && thislen < PGLZ_MAX_MATCH {
                    thislen += 1;
                    ip = ip.add(1);
                    hp = hp.add(1);
                }
            }
        } else {
            while ip < end && *ip == *hp && thislen < PGLZ_MAX_MATCH {
                thislen += 1;
                ip = ip.add(1);
                hp = hp.add(1);
            }
        }

        /*
         * Remember this match as the best (if it is)
         */
        if thislen > len {
            len = thislen;
            off = thisoff;
        }

        /*
         * Advance to the next history entry
         */
        hent = (*hent).next;

        /*
         * Be happy with lesser good matches the more entries we visited. But
         * no point in doing calculation if we're at end of list.
         */
        if hent != invalid_entry_ptr() {
            if len >= good_match {
                break;
            }
            good_match -= (good_match * good_drop) / 100;
        }
    }

    /*
     * Return match information only if it results at least in one byte
     * reduction.
     */
    if len > 2 {
        *lenp = len;
        *offp = off;
        return 1;
    }

    0
}

/* ----------
 * pglz_compress -
 *
 *		Compresses source into dest using strategy. Returns the number of
 *		bytes written in buffer dest, or -1 if compression fails.
 * ----------
 */
#[allow(non_snake_case)]
pub unsafe fn pglz_compress(
    source: *const c_char,
    slen: int32,
    dest: *mut c_char,
    mut strategy: *const PGLZ_Strategy,
) -> int32 {
    let mut bp: *mut c_uchar = dest as *mut c_uchar;
    let bstart: *mut c_uchar = bp;
    let mut hist_next: c_int = 1;
    let mut hist_recycle: bool = false;
    let mut dp: *const c_char = source;
    let dend: *const c_char = source.add(slen as usize);
    let mut ctrl_dummy: c_uchar = 0;
    let mut ctrlp: *mut c_uchar = &mut ctrl_dummy;
    let mut ctrlb: c_uchar = 0;
    let mut ctrl: c_int = 0;
    let mut found_match: bool = false;
    let mut match_len: c_int = 0;
    let mut match_off: c_int = 0;
    let mut good_match: int32;
    let mut good_drop: int32;
    let result_size: int32;
    let result_max: int32;
    let mut need_rate: int32;
    let hashsz: c_int;
    let mask: c_int;

    /*
     * Our fallback strategy is the default.
     */
    if strategy.is_null() {
        strategy = PGLZ_strategy_default;
    }

    /*
     * If the strategy forbids compression (at all or if source chunk size out
     * of range), fail.
     */
    if (*strategy).match_size_good <= 0
        || slen < (*strategy).min_input_size
        || slen > (*strategy).max_input_size
    {
        return -1;
    }

    /*
     * Limit the match parameters to the supported range.
     */
    good_match = (*strategy).match_size_good;
    if good_match > PGLZ_MAX_MATCH {
        good_match = PGLZ_MAX_MATCH;
    } else if good_match < 17 {
        good_match = 17;
    }

    good_drop = (*strategy).match_size_drop;
    if good_drop < 0 {
        good_drop = 0;
    } else if good_drop > 100 {
        good_drop = 100;
    }

    need_rate = (*strategy).min_comp_rate;
    if need_rate < 0 {
        need_rate = 0;
    } else if need_rate > 99 {
        need_rate = 99;
    }

    /*
     * Compute the maximum result size allowed by the strategy, namely the
     * input size minus the minimum wanted compression rate.  This had better
     * be <= slen, else we might overrun the provided output buffer.
     */
    if slen > (INT_MAX / 100) {
        /* Approximate to avoid overflow */
        result_max = (slen / 100) * (100 - need_rate);
    } else {
        result_max = (slen * (100 - need_rate)) / 100;
    }

    /*
     * Experiments suggest that these hash sizes work pretty well. A large
     * hash table minimizes collision, but has a higher startup cost. For a
     * small input, the startup cost dominates. The table size must be a power
     * of two.
     */
    if slen < 128 {
        hashsz = 512;
    } else if slen < 256 {
        hashsz = 1024;
    } else if slen < 512 {
        hashsz = 2048;
    } else if slen < 1024 {
        hashsz = 4096;
    } else {
        hashsz = 8192;
    }
    mask = hashsz - 1;

    let hist_start: *mut int16 = addr_of_mut!(HIST_WORK.hist_start[0]);
    let hist_entries: *mut PGLZ_HistEntry = addr_of_mut!(HIST_WORK.hist_entries[0]);

    /*
     * Initialize the history lists to empty.  We do not need to zero the
     * hist_entries[] array; its entries are initialized as they are used.
     */
    // NB: write_bytes' count is in *elements* of int16, so this zeroes
    // hashsz int16 elements (== hashsz * sizeof(int16) bytes), matching
    // C's memset(hist_start, 0, hashsz * sizeof(int16)).
    core::ptr::write_bytes(hist_start, 0, hashsz as usize);

    /*
     * Compress the source directly into the output buffer.
     */
    while dp < dend {
        /*
         * If we already exceeded the maximum result size, fail.
         *
         * We check once per loop; since the loop body could emit as many as 4
         * bytes (a control byte and 3-byte tag), PGLZ_MAX_OUTPUT() had better
         * allow 4 slop bytes.
         */
        if (bp as isize - bstart as isize) as int32 >= result_max {
            return -1;
        }

        /*
         * If we've emitted more than first_success_by bytes without finding
         * anything compressible at all, fail.  This lets us fall out
         * reasonably quickly when looking at incompressible input (such as
         * pre-compressed data).
         */
        if !found_match && (bp as isize - bstart as isize) as int32 >= (*strategy).first_success_by
        {
            return -1;
        }

        /*
         * Try to find a match in the history
         */
        if pglz_find_match(
            hist_start,
            dp,
            dend,
            &mut match_len,
            &mut match_off,
            good_match,
            good_drop,
            mask,
        ) != 0
        {
            /*
             * Create the tag and add history entries for all matched
             * characters.
             */
            pglz_out_tag(
                &mut ctrlp,
                &mut ctrlb,
                &mut ctrl,
                &mut bp,
                match_len,
                match_off,
            );
            while {
                let cur = match_len;
                match_len -= 1;
                cur
            } != 0
            {
                pglz_hist_add(
                    hist_start,
                    hist_entries,
                    &mut hist_next,
                    &mut hist_recycle,
                    dp,
                    dend,
                    mask,
                );
                dp = dp.add(1); /* Do not do this ++ in the line above! */
                /* The macro would do it four times - Jan.  */
            }
            found_match = true;
        } else {
            /*
             * No match found. Copy one literal byte.
             */
            pglz_out_literal(&mut ctrlp, &mut ctrlb, &mut ctrl, &mut bp, *dp as c_uchar);
            pglz_hist_add(
                hist_start,
                hist_entries,
                &mut hist_next,
                &mut hist_recycle,
                dp,
                dend,
                mask,
            );
            dp = dp.add(1); /* Do not do this ++ in the line above! */
            /* The macro would do it four times - Jan.  */
        }
    }

    /*
     * Write out the last control byte and check that we haven't overrun the
     * output size allowed by the strategy.
     */
    *ctrlp = ctrlb;
    result_size = (bp as isize - bstart as isize) as int32;
    if result_size >= result_max {
        return -1;
    }

    /* success */
    result_size
}

/* ----------
 * pglz_decompress -
 *
 *		Decompresses source into dest. Returns the number of bytes
 *		decompressed into the destination buffer, or -1 if the
 *		compressed data is corrupted.
 *
 *		If check_complete is true, the data is considered corrupted
 *		if we don't exactly fill the destination buffer.  Callers that
 *		are extracting a slice typically can't apply this check.
 * ----------
 */
#[allow(non_snake_case)]
pub unsafe fn pglz_decompress(
    source: *const c_char,
    slen: int32,
    dest: *mut c_char,
    rawsize: int32,
    check_complete: bool,
) -> int32 {
    let mut sp: *const c_uchar;
    let srcend: *const c_uchar;
    let mut dp: *mut c_uchar;
    let destend: *mut c_uchar;

    sp = source as *const c_uchar;
    srcend = (source as *const c_uchar).add(slen as usize);
    dp = dest as *mut c_uchar;
    destend = dp.add(rawsize as usize);

    while sp < srcend && dp < destend {
        /*
         * Read one control byte and process the next 8 items (or as many as
         * remain in the compressed input).
         */
        let mut ctrl: c_uchar = *sp;
        sp = sp.add(1);

        let mut ctrlc: c_int = 0;
        while ctrlc < 8 && sp < srcend && dp < destend {
            if (ctrl & 1) != 0 {
                /*
                 * Set control bit means we must read a match tag. The match
                 * is coded with two bytes. First byte uses lower nibble to
                 * code length - 3. Higher nibble contains upper 4 bits of the
                 * offset. The next following byte contains the lower 8 bits
                 * of the offset. If the length is coded as 18, another
                 * extension tag byte tells how much longer the match really
                 * was (0-255).
                 */
                let mut len: int32;
                let off: int32;

                len = ((*sp.add(0) as int32) & 0x0f) + 3;
                off = (((*sp.add(0) as int32) & 0xf0) << 4) | (*sp.add(1) as int32);
                sp = sp.add(2);
                if len == 18 {
                    len += *sp as int32;
                    sp = sp.add(1);
                }

                /*
                 * Check for corrupt data: if we fell off the end of the
                 * source, or if we obtained off = 0, or if off is more than
                 * the distance back to the buffer start, we have problems.
                 * (We must check for off = 0, else we risk an infinite loop
                 * below in the face of corrupt data.  Likewise, the upper
                 * limit on off prevents accessing outside the buffer
                 * boundaries.)
                 */
                if unlikely(
                    sp > srcend
                        || off == 0
                        || off as isize > (dp as isize - dest as isize),
                ) {
                    return -1;
                }

                /*
                 * Don't emit more data than requested.
                 */
                len = Min(len, (destend as isize - dp as isize) as int32);

                /*
                 * Now we copy the bytes specified by the tag from OUTPUT to
                 * OUTPUT (copy len bytes from dp - off to dp).  The copied
                 * areas could overlap, so to avoid undefined behavior in
                 * memcpy(), be careful to copy only non-overlapping regions.
                 *
                 * Note that we cannot use memmove() instead, since while its
                 * behavior is well-defined, it's also not what we want.
                 */
                let mut off = off;
                while off < len {
                    /*
                     * We can safely copy "off" bytes since that clearly
                     * results in non-overlapping source and destination.
                     */
                    core::ptr::copy_nonoverlapping(
                        dp.offset(-(off as isize)),
                        dp,
                        off as usize,
                    );
                    len -= off;
                    dp = dp.add(off as usize);

                    /*----------
                     * This bit is less obvious: we can double "off" after
                     * each such step.  Consider this raw input:
                     *		112341234123412341234
                     * This will be encoded as 5 literal bytes "11234" and
                     * then a match tag with length 16 and offset 4.  After
                     * memcpy'ing the first 4 bytes, we will have emitted
                     *		112341234
                     * so we can double "off" to 8, then after the next step
                     * we have emitted
                     *		11234123412341234
                     * Then we can double "off" again, after which it is more
                     * than the remaining "len" so we fall out of this loop
                     * and finish with a non-overlapping copy of the
                     * remainder.  In general, a match tag with off < len
                     * implies that the decoded data has a repeat length of
                     * "off".  We can handle 1, 2, 4, etc repetitions of the
                     * repeated string per memcpy until we get to a situation
                     * where the final copy step is non-overlapping.
                     *
                     * (Another way to understand this is that we are keeping
                     * the copy source point dp - off the same throughout.)
                     *----------
                     */
                    off += off;
                }
                core::ptr::copy_nonoverlapping(
                    dp.offset(-(off as isize)),
                    dp,
                    len as usize,
                );
                dp = dp.add(len as usize);
            } else {
                /*
                 * An unset control bit means LITERAL BYTE. So we just copy
                 * one from INPUT to OUTPUT.
                 */
                *dp = *sp;
                dp = dp.add(1);
                sp = sp.add(1);
            }

            /*
             * Advance the control bit
             */
            ctrl >>= 1;

            ctrlc += 1;
        }
    }

    /*
     * If requested, check we decompressed the right amount.
     */
    if check_complete && (dp != destend || sp != srcend) {
        return -1;
    }

    /*
     * That's it.
     */
    (dp as isize - dest as isize) as int32
}

/* ----------
 * pglz_maximum_compressed_size -
 *
 *		Calculate the maximum compressed size for a given amount of raw data.
 *		Return the maximum size, or total compressed size if maximum size is
 *		larger than total compressed size.
 *
 * We can't use PGLZ_MAX_OUTPUT for this purpose, because that's used to size
 * the compression buffer (and abort the compression). It does not really say
 * what's the maximum compressed size for an input of a given length, and it
 * may happen that while the whole value is compressible (and thus fits into
 * PGLZ_MAX_OUTPUT nicely), the prefix is not compressible at all.
 * ----------
 */
pub fn pglz_maximum_compressed_size(rawsize: int32, total_compressed_size: int32) -> int32 {
    let mut compressed_size: int64;

    /*
     * pglz uses one control bit per byte, so if the entire desired prefix is
     * represented as literal bytes, we'll need (rawsize * 9) bits.  We care
     * about bytes though, so be sure to round up not down.
     *
     * Use int64 here to prevent overflow during calculation.
     */
    compressed_size = ((rawsize as int64) * 9 + 7) / 8;

    /*
     * The above fails to account for a corner case: we could have compressed
     * data that starts with N-1 or N-2 literal bytes and then has a match tag
     * of 2 or 3 bytes.  It's therefore possible that we need to fetch 1 or 2
     * more bytes in order to have the whole match tag.  (Match tags earlier
     * in the compressed data don't cause a problem, since they should
     * represent more decompressed bytes than they occupy themselves.)
     */
    compressed_size += 2;

    /*
     * Maximum compressed size can't be larger than total compressed size.
     * (This also ensures that our result fits in int32.)
     */
    compressed_size = Min(compressed_size, total_compressed_size as int64);

    compressed_size as int32
}

/* ----------
 * Private helpers
 * ----------
 */

// TODO(pg-port): prelude does not export libc memcmp; provide a private
// equivalent for the memcmp() fast-path in pglz_find_match.
#[inline]
unsafe fn libc_memcmp(a: *const c_char, b: *const c_char, n: usize) -> c_int {
    let mut i: usize = 0;
    while i < n {
        let ca = *(a.add(i) as *const c_uchar);
        let cb = *(b.add(i) as *const c_uchar);
        if ca != cb {
            return (ca as c_int) - (cb as c_int);
        }
        i += 1;
    }
    0
}
