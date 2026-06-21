//! regexp.rs
//!   Postgres' interface to the regular expression package.
//! Translated 1:1 from postgres/src/backend/utils/adt/regexp.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/regexp.c
//!
//!     Alistair Crooks added the code for the regex caching
//!     agc - cached the regular expressions used - there's a good chance
//!     that we'll get a hit, so this saves a compile step for every
//!     attempted match. I haven't actually measured the speed improvement,
//!     but it `looks' a lot quicker visually when watching regression
//!     test output.
//!
//!     agc - incorporated Keith Bostic's Berkeley regex code into
//!     the tree for all ports. To distinguish this regex code from any that
//!     is existent on a platform, I've prepended the string "pg_" to
//!     the functions regcomp, regerror, regexec and regfree.
//!     Fixed a bug that was originally a typo by me, where `i' was used
//!     instead of `oldest' when compiling regular expressions - benign
//!     results mostly, although occasionally it bit you...

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*; // postgres.h

use crate::catalog::pg_type_d::TEXTOID;
use crate::catalog::pg_type::TYPALIGN_INT;
use crate::c::{int64, text, Size};
use crate::mb::mbutils::{
    pg_database_encoding_max_length, pg_mb2wchar_with_len, pg_mblen_range, pg_mbstrlen_with_len,
    pg_wchar2mb_with_len,
};
use crate::mb::pg_wchar::pg_wchar;
use crate::postgres::{Int32GetDatum, PointerGetDatum};
use crate::c::NameStr;
use crate::regex::regex::{
    pg_regcomp, pg_regerror, pg_regexec, pg_regprefix, regex_t, regmatch_t, REG_ADVANCED,
    REG_EXACT, REG_EXPANDED, REG_EXTENDED, REG_ICASE, REG_NEWLINE, REG_NLANCH, REG_NLSTOP,
    REG_NOMATCH, REG_NOSUB, REG_OKAY, REG_PREFIX, REG_QUOTE,
};
use crate::utils::adt::varlena::cstring_to_text_with_len;
use crate::utils::array::ArrayType;
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::mmgr::mcxt::MemoryContextSetParent;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::{
    DirectFunctionCall3,
    PG_ARGISNULL, PG_GETARG_INT32, PG_GETARG_NAME, PG_GETARG_TEXT_P_COPY, PG_GETARG_TEXT_PP,
    PG_GET_COLLATION, PG_NARGS, PG_RETURN_BOOL, PG_RETURN_DATUM, PG_RETURN_INT32, PG_RETURN_NULL,
    PG_RETURN_TEXT_P,
};

// c_char / c_int / c_void / c_long come from the prelude (core::ffi).

// VARHDRSZ comes from c.h (varatt re-exports it).
use crate::c::VARHDRSZ;

// SRF_* macros (funcapi.h). The srf_* helper fns and FuncCallContext they
// expand to are defined near the bottom of this file (fns/structs are
// order-independent); the macros must be defined before their first use.
macro_rules! SRF_IS_FIRSTCALL {
    ($fcinfo:expr) => {
        srf_is_firstcall($fcinfo)
    };
}
macro_rules! SRF_FIRSTCALL_INIT {
    ($fcinfo:expr) => {
        srf_firstcall_init($fcinfo)
    };
}
macro_rules! SRF_PERCALL_SETUP {
    ($fcinfo:expr) => {
        srf_percall_setup($fcinfo)
    };
}
macro_rules! SRF_RETURN_NEXT {
    ($fcinfo:expr, $fctx:expr, $result:expr) => {
        return srf_return_next($fcinfo, $fctx, $result)
    };
}
macro_rules! SRF_RETURN_DONE {
    ($fcinfo:expr, $fctx:expr) => {
        return srf_return_done($fcinfo, $fctx)
    };
}

/*
 * PG_GETARG_TEXT_PP_IF_EXISTS(_n) returns the n'th text arg if present, else NULL.
 */
macro_rules! PG_GETARG_TEXT_PP_IF_EXISTS {
    ($fcinfo:expr, $n:expr) => {
        if (PG_NARGS!($fcinfo) as c_int) > ($n) {
            PG_GETARG_TEXT_PP!($fcinfo, $n)
        } else {
            std::ptr::null_mut()
        }
    };
}

/* all the options of interest for regex functions */
#[repr(C)]
pub struct pg_re_flags {
    pub cflags: c_int, /* compile flags for Spencer's regex code */
    pub glob: bool,    /* do it globally (for each occurrence) */
}

/* cross-call state for regexp_match and regexp_split functions */
#[repr(C)]
pub struct regexp_matches_ctx {
    pub orig_str: *mut text, /* data string in original TEXT form */
    pub nmatches: c_int,     /* number of places where pattern matched */
    pub npatterns: c_int,    /* number of capturing subpatterns */
    /* We store start char index and end+1 char index for each match */
    /* so the number of entries in match_locs is nmatches * npatterns * 2 */
    pub match_locs: *mut c_int, /* 0-based character indexes */
    pub next_match: c_int,      /* 0-based index of next match to process */
    /* workspace for build_regexp_match_result() */
    pub elems: *mut Datum, /* has npatterns elements */
    pub nulls: *mut bool,  /* has npatterns elements */
    pub wide_str: *mut pg_wchar, /* wide-char version of original string */
    pub conv_buf: *mut c_char, /* conversion buffer, if needed */
    pub conv_bufsiz: c_int, /* size thereof */
}

/*
 * We cache precompiled regular expressions using a "self organizing list"
 * structure, in which recently-used items tend to be near the front.
 * Whenever we use an entry, it's moved up to the front of the list.
 * Over time, an item's average position corresponds to its frequency of use.
 *
 * When we first create an entry, it's inserted at the front of
 * the array, dropping the entry at the end of the array if necessary to
 * make room.  (This might seem to be weighting the new entry too heavily,
 * but if we insert new entries further back, we'll be unable to adjust to
 * a sudden shift in the query mix where we are presented with MAX_CACHED_RES
 * never-before-seen items used circularly.  We ought to be able to handle
 * that case, so we have to insert at the front.)
 *
 * Knuth mentions a variant strategy in which a used item is moved up just
 * one place in the list.  Although he says this uses fewer comparisons on
 * average, it seems not to adapt very well to the situation where you have
 * both some reusable patterns and a steady stream of non-reusable patterns.
 * A reusable pattern that isn't used at least as often as non-reusable
 * patterns are seen will "fail to keep up" and will drop off the end of the
 * cache.  With move-to-front, a reusable pattern is guaranteed to stay in
 * the cache as long as it's used at least once in every MAX_CACHED_RES uses.
 */

/* this is the maximum number of cached regular expressions */
const MAX_CACHED_RES: usize = 32;

/* A parent memory context for regular expressions. */
static mut RegexpCacheMemoryContext: MemoryContext = std::ptr::null_mut();

/* this structure describes one cached regular expression */
#[repr(C)]
struct cached_re_str {
    cre_context: MemoryContext, /* memory context for this regexp */
    cre_pat: *mut c_char,       /* original RE (not null terminated!) */
    cre_pat_len: c_int,         /* length of original RE, in bytes */
    cre_flags: c_int,           /* compile flags: extended,icase etc */
    cre_collation: Oid,         /* collation to use */
    cre_re: regex_t,            /* the compiled regular expression */
}

static mut num_res: c_int = 0; /* # of cached re's */
static mut re_array: [cached_re_str; MAX_CACHED_RES] =
    [const { unsafe { std::mem::zeroed() } }; MAX_CACHED_RES]; /* cached re's */

/*
 * RE_compile_and_cache - compile a RE, caching if possible
 *
 * Returns regex_t *
 *
 *	text_re --- the pattern, expressed as a TEXT object
 *	cflags --- compile options for the pattern
 *	collation --- collation to use for LC_CTYPE-dependent behavior
 *
 * Pattern is given in the database encoding.  We internally convert to
 * an array of pg_wchar, which is what Spencer's regex package wants.
 */
pub unsafe fn RE_compile_and_cache(text_re: *mut text, cflags: c_int, collation: Oid) -> *mut regex_t {
    let text_re_len: c_int = VARSIZE_ANY_EXHDR(text_re as *const c_char) as c_int;
    let text_re_val: *mut c_char = VARDATA_ANY(text_re as *const c_char);
    let pattern: *mut pg_wchar;
    let pattern_len: c_int;
    let regcomp_result: c_int;
    let mut re_temp: cached_re_str = std::mem::zeroed();
    let mut errMsg: [c_char; 100] = [0; 100];
    let oldcontext: MemoryContext;

    /*
     * Look for a match among previously compiled REs.  Since the data
     * structure is self-organizing with most-used entries at the front, our
     * search strategy can just be to scan from the front.
     */
    {
        let mut i: c_int = 0;
        while i < num_res {
            let iu = i as usize;
            if re_array[iu].cre_pat_len == text_re_len
                && re_array[iu].cre_flags == cflags
                && re_array[iu].cre_collation == collation
                && libc_memcmp(re_array[iu].cre_pat, text_re_val, text_re_len as usize) == 0
            {
                /*
                 * Found a match; move it to front if not there already.
                 */
                if i > 0 {
                    re_temp = core::ptr::read(&raw const re_array[iu]);
                    libc_memmove(
                        &raw mut re_array[1] as *mut c_void,
                        &raw const re_array[0] as *const c_void,
                        (i as usize) * std::mem::size_of::<cached_re_str>(),
                    );
                    core::ptr::write(&raw mut re_array[0], re_temp);
                }

                return &raw mut re_array[0].cre_re;
            }
            i += 1;
        }
    }

    /* Set up the cache memory on first go through. */
    if unlikely(RegexpCacheMemoryContext.is_null()) {
        RegexpCacheMemoryContext = AllocSetContextCreate!(
            TopMemoryContext,
            c"RegexpCacheMemoryContext".as_ptr(),
            ALLOCSET_SMALL_SIZES
        );
    }

    /*
     * Couldn't find it, so try to compile the new RE.  To avoid leaking
     * resources on failure, we build into the re_temp local.
     */

    /* Convert pattern string to wide characters */
    pattern = palloc(((text_re_len + 1) as Size) * std::mem::size_of::<pg_wchar>()) as *mut pg_wchar;
    pattern_len = pg_mb2wchar_with_len(text_re_val, pattern, text_re_len);

    /*
     * Make a memory context for this compiled regexp.  This is initially a
     * child of the current memory context, so it will be cleaned up
     * automatically if compilation is interrupted and throws an ERROR. We'll
     * re-parent it under the longer lived cache context if we make it to the
     * bottom of this function.
     */
    // NOTE(pg-port): with the context-less bootstrap allocator AllocSetContextCreate!
    // returns its parent, so creating under CurrentMemoryContext (a per-tuple context)
    // and later MemoryContextSetParent'ing into the cache reparents the per-tuple
    // context itself, corrupting the context tree (infinite loop on scan reset).
    // Create directly under the long-lived cache context and skip the reparent.
    re_temp.cre_context =
        AllocSetContextCreate!(RegexpCacheMemoryContext, c"RegexpMemoryContext".as_ptr(), ALLOCSET_SMALL_SIZES);
    oldcontext = MemoryContextSwitchTo(re_temp.cre_context);

    regcomp_result = pg_regcomp(
        &raw mut re_temp.cre_re,
        pattern,
        pattern_len as Size,
        cflags,
        collation,
    );

    pfree(pattern as *mut c_void);

    if regcomp_result != REG_OKAY {
        /* re didn't compile (no need for pg_regfree, if so) */
        pg_regerror(
            regcomp_result,
            &raw const re_temp.cre_re,
            errMsg.as_mut_ptr(),
            std::mem::size_of_val(&errMsg) as Size,
        );
        ereport!(
            ERROR,
            errmsg!(
                "invalid regular expression: {}",
                std::ffi::CStr::from_ptr(errMsg.as_ptr()).to_string_lossy()
            )
        );
    }

    /* Copy the pattern into the per-regexp memory context. */
    re_temp.cre_pat = palloc((text_re_len + 1) as Size) as *mut c_char;
    libc_memcpy(re_temp.cre_pat, text_re_val, text_re_len as usize);

    /*
     * NUL-terminate it only for the benefit of the identifier used for the
     * memory context, visible in the pg_backend_memory_contexts view.
     */
    *re_temp.cre_pat.add(text_re_len as usize) = 0;
    MemoryContextSetIdentifier(re_temp.cre_context, re_temp.cre_pat);

    re_temp.cre_pat_len = text_re_len;
    re_temp.cre_flags = cflags;
    re_temp.cre_collation = collation;

    /*
     * Okay, we have a valid new item in re_temp; insert it into the storage
     * array.  Discard last entry if needed.
     */
    if num_res >= MAX_CACHED_RES as c_int {
        num_res -= 1;
        Assert!(num_res < MAX_CACHED_RES as c_int);
        /* Delete the memory context holding the regexp and pattern. */
        MemoryContextDelete(re_array[num_res as usize].cre_context);
    }

    /* Already created under the long-lived cache context above; no reparent. */

    if num_res > 0 {
        libc_memmove(
            &raw mut re_array[1] as *mut c_void,
            &raw const re_array[0] as *const c_void,
            (num_res as usize) * std::mem::size_of::<cached_re_str>(),
        );
    }

    re_array[0] = re_temp;
    num_res += 1;

    MemoryContextSwitchTo(oldcontext);

    &raw mut re_array[0].cre_re
}

/*
 * RE_wchar_execute - execute a RE on pg_wchar data
 *
 * Returns true on match, false on no match
 *
 *	re --- the compiled pattern as returned by RE_compile_and_cache
 *	data --- the data to match against (need not be null-terminated)
 *	data_len --- the length of the data string
 *	start_search -- the offset in the data to start searching
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Data is given as array of pg_wchar which is what Spencer's regex package
 * wants.
 */
unsafe fn RE_wchar_execute(
    re: *mut regex_t,
    data: *mut pg_wchar,
    data_len: c_int,
    start_search: c_int,
    nmatch: c_int,
    pmatch: *mut regmatch_t,
) -> bool {
    let regexec_result: c_int;
    let mut errMsg: [c_char; 100] = [0; 100];

    /* Perform RE match and return result */
    regexec_result = pg_regexec(
        re,
        data,
        data_len as Size,
        start_search as Size,
        std::ptr::null_mut(), /* no details */
        nmatch as Size,
        pmatch,
        0,
    );

    if regexec_result != REG_OKAY && regexec_result != REG_NOMATCH {
        /* re failed??? */
        pg_regerror(
            regexec_result,
            re,
            errMsg.as_mut_ptr(),
            std::mem::size_of_val(&errMsg) as Size,
        );
        ereport!(
            ERROR,
            errmsg!(
                "regular expression failed: {}",
                std::ffi::CStr::from_ptr(errMsg.as_ptr()).to_string_lossy()
            )
        );
    }

    regexec_result == REG_OKAY
}

/*
 * RE_execute - execute a RE
 *
 * Returns true on match, false on no match
 *
 *	re --- the compiled pattern as returned by RE_compile_and_cache
 *	dat --- the data to match against (need not be null-terminated)
 *	dat_len --- the length of the data string
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Data is given in the database encoding.  We internally
 * convert to array of pg_wchar which is what Spencer's regex package wants.
 */
unsafe fn RE_execute(
    re: *mut regex_t,
    dat: *mut c_char,
    dat_len: c_int,
    nmatch: c_int,
    pmatch: *mut regmatch_t,
) -> bool {
    let data: *mut pg_wchar;
    let data_len: c_int;
    let match_: bool;

    /* Convert data string to wide characters */
    data = palloc(((dat_len + 1) as Size) * std::mem::size_of::<pg_wchar>()) as *mut pg_wchar;
    data_len = pg_mb2wchar_with_len(dat, data, dat_len);

    /* Perform RE match and return result */
    match_ = RE_wchar_execute(re, data, data_len, 0, nmatch, pmatch);

    pfree(data as *mut c_void);
    match_
}

/*
 * RE_compile_and_execute - compile and execute a RE
 *
 * Returns true on match, false on no match
 *
 *	text_re --- the pattern, expressed as a TEXT object
 *	dat --- the data to match against (need not be null-terminated)
 *	dat_len --- the length of the data string
 *	cflags --- compile options for the pattern
 *	collation --- collation to use for LC_CTYPE-dependent behavior
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Both pattern and data are given in the database encoding.  We internally
 * convert to array of pg_wchar which is what Spencer's regex package wants.
 */
pub unsafe fn RE_compile_and_execute(
    text_re: *mut text,
    dat: *mut c_char,
    dat_len: c_int,
    mut cflags: c_int,
    collation: Oid,
    nmatch: c_int,
    pmatch: *mut regmatch_t,
) -> bool {
    let re: *mut regex_t;

    /* Use REG_NOSUB if caller does not want sub-match details */
    if nmatch < 2 {
        cflags |= REG_NOSUB;
    }

    /* Compile RE */
    re = RE_compile_and_cache(text_re, cflags, collation);

    RE_execute(re, dat, dat_len, nmatch, pmatch)
}

/*
 * parse_re_flags - parse the options argument of regexp_match and friends
 *
 *	flags --- output argument, filled with desired options
 *	opts --- TEXT object, or NULL for defaults
 *
 * This accepts all the options allowed by any of the callers; callers that
 * don't want some have to reject them after the fact.
 */
unsafe fn parse_re_flags(flags: *mut pg_re_flags, opts: *mut text) {
    /* regex flavor is always folded into the compile flags */
    (*flags).cflags = REG_ADVANCED;
    (*flags).glob = false;

    if !opts.is_null() {
        let opt_p: *mut c_char = VARDATA_ANY(opts as *const c_char);
        let opt_len: c_int = VARSIZE_ANY_EXHDR(opts as *const c_char) as c_int;
        let mut i: c_int = 0;

        while i < opt_len {
            match *opt_p.add(i as usize) as u8 as char {
                'g' => {
                    (*flags).glob = true;
                }
                'b' => {
                    /* BREs (but why???) */
                    (*flags).cflags &= !(REG_ADVANCED | REG_EXTENDED | REG_QUOTE);
                }
                'c' => {
                    /* case sensitive */
                    (*flags).cflags &= !REG_ICASE;
                }
                'e' => {
                    /* plain EREs */
                    (*flags).cflags |= REG_EXTENDED;
                    (*flags).cflags &= !(REG_ADVANCED | REG_QUOTE);
                }
                'i' => {
                    /* case insensitive */
                    (*flags).cflags |= REG_ICASE;
                }
                'm' | 'n' => {
                    /* 'm': Perloid synonym for n; 'n': \n affects ^ $ . [^ */
                    (*flags).cflags |= REG_NEWLINE;
                }
                'p' => {
                    /* ~Perl, \n affects . [^ */
                    (*flags).cflags |= REG_NLSTOP;
                    (*flags).cflags &= !REG_NLANCH;
                }
                'q' => {
                    /* literal string */
                    (*flags).cflags |= REG_QUOTE;
                    (*flags).cflags &= !(REG_ADVANCED | REG_EXTENDED);
                }
                's' => {
                    /* single line, \n ordinary */
                    (*flags).cflags &= !REG_NEWLINE;
                }
                't' => {
                    /* tight syntax */
                    (*flags).cflags &= !REG_EXPANDED;
                }
                'w' => {
                    /* weird, \n affects ^ $ only */
                    (*flags).cflags &= !REG_NLSTOP;
                    (*flags).cflags |= REG_NLANCH;
                }
                'x' => {
                    /* expanded syntax */
                    (*flags).cflags |= REG_EXPANDED;
                }
                _ => {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "invalid regular expression option: \"{}\"",
                            mblen_slice(opt_p.add(i as usize), opt_p.add(opt_len as usize))
                        )
                    );
                }
            }
            i += 1;
        }
    }
}

/*
 *	interface routines called by the function manager
 */

pub unsafe fn nameregexeq(fcinfo: FunctionCallInfo) -> Datum {
    let n: Name = PG_GETARG_NAME!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

    PG_RETURN_BOOL!(RE_compile_and_execute(
        p,
        NameStr(&*n) as *mut c_char,
        strlen(NameStr(&*n)) as c_int,
        REG_ADVANCED,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

pub unsafe fn nameregexne(fcinfo: FunctionCallInfo) -> Datum {
    let n: Name = PG_GETARG_NAME!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

    PG_RETURN_BOOL!(!RE_compile_and_execute(
        p,
        NameStr(&*n) as *mut c_char,
        strlen(NameStr(&*n)) as c_int,
        REG_ADVANCED,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

pub unsafe fn textregexeq(fcinfo: FunctionCallInfo) -> Datum {
    if std::env::var_os("PDB_RX").is_some() { eprintln!("PDB_RX textregexeq EXEC reached"); }
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

    PG_RETURN_BOOL!(RE_compile_and_execute(
        p,
        VARDATA_ANY(s as *const c_char),
        VARSIZE_ANY_EXHDR(s as *const c_char) as c_int,
        REG_ADVANCED,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

pub unsafe fn textregexne(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

    PG_RETURN_BOOL!(!RE_compile_and_execute(
        p,
        VARDATA_ANY(s as *const c_char),
        VARSIZE_ANY_EXHDR(s as *const c_char) as c_int,
        REG_ADVANCED,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

/*
 *	routines that use the regexp stuff, but ignore the case.
 *	for this, we use the REG_ICASE flag to pg_regcomp
 */

pub unsafe fn nameicregexeq(fcinfo: FunctionCallInfo) -> Datum {
    let n: Name = PG_GETARG_NAME!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

    PG_RETURN_BOOL!(RE_compile_and_execute(
        p,
        NameStr(&*n) as *mut c_char,
        strlen(NameStr(&*n)) as c_int,
        REG_ADVANCED | REG_ICASE,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

pub unsafe fn nameicregexne(fcinfo: FunctionCallInfo) -> Datum {
    let n: Name = PG_GETARG_NAME!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

    PG_RETURN_BOOL!(!RE_compile_and_execute(
        p,
        NameStr(&*n) as *mut c_char,
        strlen(NameStr(&*n)) as c_int,
        REG_ADVANCED | REG_ICASE,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

pub unsafe fn texticregexeq(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

    PG_RETURN_BOOL!(RE_compile_and_execute(
        p,
        VARDATA_ANY(s as *const c_char),
        VARSIZE_ANY_EXHDR(s as *const c_char) as c_int,
        REG_ADVANCED | REG_ICASE,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

pub unsafe fn texticregexne(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);

    PG_RETURN_BOOL!(!RE_compile_and_execute(
        p,
        VARDATA_ANY(s as *const c_char),
        VARSIZE_ANY_EXHDR(s as *const c_char) as c_int,
        REG_ADVANCED | REG_ICASE,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

/*
 * textregexsubstr()
 *		Return a substring matched by a regular expression.
 */
pub unsafe fn textregexsubstr(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let re: *mut regex_t;
    let mut pmatch: [regmatch_t; 2] = std::mem::zeroed();
    let so: c_int;
    let eo: c_int;

    /* Compile RE */
    re = RE_compile_and_cache(p, REG_ADVANCED, PG_GET_COLLATION!(fcinfo));

    /*
     * We pass two regmatch_t structs to get info about the overall match and
     * the match for the first parenthesized subexpression (if any). If there
     * is a parenthesized subexpression, we return what it matched; else
     * return what the whole regexp matched.
     */
    if !RE_execute(
        re,
        VARDATA_ANY(s as *const c_char),
        VARSIZE_ANY_EXHDR(s as *const c_char) as c_int,
        2,
        pmatch.as_mut_ptr(),
    ) {
        PG_RETURN_NULL!(fcinfo); /* definitely no match */
    }

    if (*re).re_nsub > 0 {
        /* has parenthesized subexpressions, use the first one */
        so = pmatch[1].rm_so as c_int;
        eo = pmatch[1].rm_eo as c_int;
    } else {
        /* no parenthesized subexpression, use whole match */
        so = pmatch[0].rm_so as c_int;
        eo = pmatch[0].rm_eo as c_int;
    }

    /*
     * It is possible to have a match to the whole pattern but no match for a
     * subexpression; for example 'foo(bar)?' is considered to match 'foo' but
     * there is no subexpression match.  So this extra test for match failure
     * is not redundant.
     */
    if so < 0 || eo < 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    DirectFunctionCall3!(
        text_substr,
        PointerGetDatum(s as *const c_void),
        Int32GetDatum(so + 1),
        Int32GetDatum(eo - so)
    )
}

/*
 * textregexreplace_noopt()
 *		Return a string matched by a regular expression, with replacement.
 *
 * This version doesn't have an option argument: we default to case
 * sensitive match, replace the first instance only.
 */
pub unsafe fn textregexreplace_noopt(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let r: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 2);

    PG_RETURN_TEXT_P!(replace_text_regexp(
        s,
        p,
        r,
        REG_ADVANCED,
        PG_GET_COLLATION!(fcinfo),
        0,
        1
    ));
}

/*
 * textregexreplace()
 *		Return a string matched by a regular expression, with replacement.
 */
pub unsafe fn textregexreplace(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let r: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 2);
    let opt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 3);
    let mut flags: pg_re_flags = std::mem::zeroed();

    /*
     * regexp_replace() with four arguments will be preferentially resolved as
     * this form when the fourth argument is of type UNKNOWN.  However, the
     * user might have intended to call textregexreplace_extended_no_n.  If we
     * see flags that look like an integer, emit the same error that
     * parse_re_flags would, but add a HINT about how to fix it.
     */
    if VARSIZE_ANY_EXHDR(opt as *const c_char) as c_int > 0 {
        let opt_p: *mut c_char = VARDATA_ANY(opt as *const c_char);
        let end_p: *const c_char = opt_p.add(VARSIZE_ANY_EXHDR(opt as *const c_char) as usize);

        if *opt_p as u8 >= b'0' && *opt_p as u8 <= b'9' {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid regular expression option: \"{}\"",
                    mblen_slice(opt_p, end_p)
                )
            );
        }
    }

    parse_re_flags(&raw mut flags, opt);

    PG_RETURN_TEXT_P!(replace_text_regexp(
        s,
        p,
        r,
        flags.cflags,
        PG_GET_COLLATION!(fcinfo),
        0,
        if flags.glob { 0 } else { 1 }
    ));
}

/*
 * textregexreplace_extended()
 *		Return a string matched by a regular expression, with replacement.
 *		Extends textregexreplace by allowing a start position and the
 *		choice of the occurrence to replace (0 means all occurrences).
 */
pub unsafe fn textregexreplace_extended(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let p: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let r: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 2);
    let mut start: c_int = 1;
    let mut n: c_int = 1;
    let flags: *mut text = PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 5);
    let mut re_flags: pg_re_flags = std::mem::zeroed();

    /* Collect optional parameters */
    if (PG_NARGS!(fcinfo) as c_int) > 3 {
        start = PG_GETARG_INT32!(fcinfo, 3);
        if start <= 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "start", start)
            );
        }
    }
    if (PG_NARGS!(fcinfo) as c_int) > 4 {
        n = PG_GETARG_INT32!(fcinfo, 4);
        if n < 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "n", n)
            );
        }
    }

    /* Determine options */
    parse_re_flags(&raw mut re_flags, flags);

    /* If N was not specified, deduce it from the 'g' flag */
    if (PG_NARGS!(fcinfo) as c_int) <= 4 {
        n = if re_flags.glob { 0 } else { 1 };
    }

    /* Do the replacement(s) */
    PG_RETURN_TEXT_P!(replace_text_regexp(
        s,
        p,
        r,
        re_flags.cflags,
        PG_GET_COLLATION!(fcinfo),
        start - 1,
        n
    ));
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn textregexreplace_extended_no_n(fcinfo: FunctionCallInfo) -> Datum {
    textregexreplace_extended(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn textregexreplace_extended_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    textregexreplace_extended(fcinfo)
}

/*
 * similar_to_escape(), similar_escape()
 *
 * Convert a SQL "SIMILAR TO" regexp pattern to POSIX style, so it can be
 * used by our regexp engine.
 *
 * similar_escape_internal() is the common workhorse for three SQL-exposed
 * functions.  esc_text can be passed as NULL to select the default escape
 * (which is '\'), or as an empty string to select no escape character.
 */
unsafe fn similar_escape_internal(pat_text: *mut text, esc_text: *mut text) -> *mut text {
    let result: *mut text;
    let mut p: *mut c_char;
    let mut e: *mut c_char;
    let mut r: *mut c_char;
    let mut plen: c_int;
    let elen: c_int;
    let pend: *const c_char;
    let mut afterescape: bool = false;
    let mut nquotes: c_int = 0;
    let mut bracket_depth: c_int = 0; /* square bracket nesting level */
    let mut charclass_pos: c_int = 0; /* position inside a character class */

    p = VARDATA_ANY(pat_text as *const c_char);
    plen = VARSIZE_ANY_EXHDR(pat_text as *const c_char) as c_int;
    pend = p.add(plen as usize);
    if esc_text.is_null() {
        /* No ESCAPE clause provided; default to backslash as escape */
        e = c"\\".as_ptr() as *mut c_char;
        elen = 1;
    } else {
        e = VARDATA_ANY(esc_text as *const c_char);
        elen = VARSIZE_ANY_EXHDR(esc_text as *const c_char) as c_int;
        if elen == 0 {
            e = std::ptr::null_mut(); /* no escape character */
        } else if elen > 1 {
            let escape_mblen: c_int = pg_mbstrlen_with_len(e, elen);

            if escape_mblen > 1 {
                ereport!(ERROR, errmsg!("invalid escape string"));
            }
        }
    }

    /*----------
     * We surround the transformed input string with
     *			^(?: ... )$
     * which requires some explanation.  We need "^" and "$" to force
     * the pattern to match the entire input string as per the SQL spec.
     * The "(?:" and ")" are a non-capturing set of parens; we have to have
     * parens in case the string contains "|", else the "^" and "$" will
     * be bound into the first and last alternatives which is not what we
     * want, and the parens must be non capturing because we don't want them
     * to count when selecting output for SUBSTRING.
     *
     * When the pattern is divided into three parts by escape-double-quotes,
     * what we emit is
     *			^(?:part1){1,1}?(part2){1,1}(?:part3)$
     * which requires even more explanation.  The "{1,1}?" on part1 makes it
     * non-greedy so that it will match the smallest possible amount of text
     * not the largest, as required by SQL.  The plain parens around part2
     * are capturing parens so that that part is what controls the result of
     * SUBSTRING.  The "{1,1}" forces part2 to be greedy, so that it matches
     * the largest possible amount of text; hence part3 must match the
     * smallest amount of text, as required by SQL.  We don't need an explicit
     * greediness marker on part3.  Note that this also confines the effects
     * of any "|" characters to the respective part, which is what we want.
     *
     * The SQL spec says that SUBSTRING's pattern must contain exactly two
     * escape-double-quotes, but we only complain if there's more than two.
     * With none, we act as though part1 and part3 are empty; with one, we
     * act as though part3 is empty.  Both behaviors fall out of omitting
     * the relevant part separators in the above expansion.  If the result
     * of this function is used in a plain regexp match (SIMILAR TO), the
     * escape-double-quotes have no effect on the match behavior.
     *
     * While we don't fully validate character classes (bracket expressions),
     * we do need to parse them well enough to know where they end.
     * "charclass_pos" tracks where we are in a character class.
     * Its value is uninteresting when bracket_depth is 0.
     * But when bracket_depth > 0, it will be
     *   1: right after the opening '[' (a following '^' will negate
     *      the class, while ']' is a literal character)
     *   2: right after a '^' after the opening '[' (']' is still a literal
     *      character)
     *   3 or more: further inside the character class (']' ends the class)
     *----------
     */

    /*
     * We need room for the prefix/postfix and part separators, plus as many
     * as 3 output bytes per input byte; since the input is at most 1GB this
     * can't overflow size_t.
     */
    result =
        palloc((VARHDRSZ as Size) + 23 + 3 * (plen as Size)) as *mut text;
    r = VARDATA(result as *const c_char);

    *r = b'^' as c_char;
    r = r.add(1);
    *r = b'(' as c_char;
    r = r.add(1);
    *r = b'?' as c_char;
    r = r.add(1);
    *r = b':' as c_char;
    r = r.add(1);

    while plen > 0 {
        let pchar: c_char = *p;

        /*
         * If both the escape character and the current character from the
         * pattern are multi-byte, we need to take the slow path.
         *
         * But if one of them is single-byte, we can process the pattern one
         * byte at a time, ignoring multi-byte characters.  (This works
         * because all server-encodings have the property that a valid
         * multi-byte character representation cannot contain the
         * representation of a valid single-byte character.)
         */

        if elen > 1 {
            let mblen: c_int = pg_mblen_range(p, pend);

            if mblen > 1 {
                /* slow, multi-byte path */
                if afterescape {
                    *r = b'\\' as c_char;
                    r = r.add(1);
                    libc_memcpy(r, p, mblen as usize);
                    r = r.add(mblen as usize);
                    afterescape = false;
                } else if !e.is_null()
                    && elen == mblen
                    && libc_memcmp(e, p, mblen as usize) == 0
                {
                    /* SQL escape character; do not send to output */
                    afterescape = true;
                } else {
                    /*
                     * We know it's a multi-byte character, so we don't need
                     * to do all the comparisons to single-byte characters
                     * that we do below.
                     */
                    libc_memcpy(r, p, mblen as usize);
                    r = r.add(mblen as usize);
                }

                p = p.add(mblen as usize);
                plen -= mblen;

                continue;
            }
        }

        /* fast path */
        if afterescape {
            if pchar == b'"' as c_char && bracket_depth < 1 {
                /* escape-double-quote? */
                /* emit appropriate part separator, per notes above */
                if nquotes == 0 {
                    *r = b')' as c_char;
                    r = r.add(1);
                    *r = b'{' as c_char;
                    r = r.add(1);
                    *r = b'1' as c_char;
                    r = r.add(1);
                    *r = b',' as c_char;
                    r = r.add(1);
                    *r = b'1' as c_char;
                    r = r.add(1);
                    *r = b'}' as c_char;
                    r = r.add(1);
                    *r = b'?' as c_char;
                    r = r.add(1);
                    *r = b'(' as c_char;
                    r = r.add(1);
                } else if nquotes == 1 {
                    *r = b')' as c_char;
                    r = r.add(1);
                    *r = b'{' as c_char;
                    r = r.add(1);
                    *r = b'1' as c_char;
                    r = r.add(1);
                    *r = b',' as c_char;
                    r = r.add(1);
                    *r = b'1' as c_char;
                    r = r.add(1);
                    *r = b'}' as c_char;
                    r = r.add(1);
                    *r = b'(' as c_char;
                    r = r.add(1);
                    *r = b'?' as c_char;
                    r = r.add(1);
                    *r = b':' as c_char;
                    r = r.add(1);
                } else {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "SQL regular expression may not contain more than two escape-double-quote separators"
                        )
                    );
                }
                nquotes += 1;
            } else {
                /*
                 * We allow any character at all to be escaped; notably, this
                 * allows access to POSIX character-class escapes such as
                 * "\d".  The SQL spec is considerably more restrictive.
                 */
                *r = b'\\' as c_char;
                r = r.add(1);
                *r = pchar;
                r = r.add(1);

                /*
                 * If we encounter an escaped character in a character class,
                 * we are no longer at the beginning.
                 */
                charclass_pos = 3;
            }
            afterescape = false;
        } else if !e.is_null() && pchar == *e {
            /* SQL escape character; do not send to output */
            afterescape = true;
        } else if bracket_depth > 0 {
            /* inside a character class */
            if pchar == b'\\' as c_char {
                /*
                 * If we're here, backslash is not the SQL escape character,
                 * so treat it as a literal class element, which requires
                 * doubling it.  (This matches our behavior for backslashes
                 * outside character classes.)
                 */
                *r = b'\\' as c_char;
                r = r.add(1);
            }
            *r = pchar;
            r = r.add(1);

            /* parse the character class well enough to identify ending ']' */
            if pchar == b']' as c_char && charclass_pos > 2 {
                /* found the real end of a bracket pair */
                bracket_depth -= 1;
                /* don't reset charclass_pos, this may be an inner bracket */
            } else if pchar == b'[' as c_char {
                /* start of a nested bracket pair */
                bracket_depth += 1;

                /*
                 * We are no longer at the beginning of a character class.
                 * (The nested bracket pair is a collating element, not a
                 * character class in its own right.)
                 */
                charclass_pos = 3;
            } else if pchar == b'^' as c_char {
                /*
                 * A caret right after the opening bracket negates the
                 * character class.  In that case, the following will
                 * increment charclass_pos from 1 to 2, so that a following
                 * ']' is still a literal character and does not end the
                 * character class.  If we are further inside a character
                 * class, charclass_pos might get incremented past 3, which is
                 * fine.
                 */
                charclass_pos += 1;
            } else {
                /*
                 * Anything else (including a backslash or leading ']') is an
                 * element of the character class, so we are no longer at the
                 * beginning of the class.
                 */
                charclass_pos = 3;
            }
        } else if pchar == b'[' as c_char {
            /* start of a character class */
            *r = pchar;
            r = r.add(1);
            bracket_depth = 1;
            charclass_pos = 1;
        } else if pchar == b'%' as c_char {
            *r = b'.' as c_char;
            r = r.add(1);
            *r = b'*' as c_char;
            r = r.add(1);
        } else if pchar == b'_' as c_char {
            *r = b'.' as c_char;
            r = r.add(1);
        } else if pchar == b'(' as c_char {
            /* convert to non-capturing parenthesis */
            *r = b'(' as c_char;
            r = r.add(1);
            *r = b'?' as c_char;
            r = r.add(1);
            *r = b':' as c_char;
            r = r.add(1);
        } else if pchar == b'\\' as c_char
            || pchar == b'.' as c_char
            || pchar == b'^' as c_char
            || pchar == b'$' as c_char
        {
            *r = b'\\' as c_char;
            r = r.add(1);
            *r = pchar;
            r = r.add(1);
        } else {
            *r = pchar;
            r = r.add(1);
        }
        p = p.add(1);
        plen -= 1;
    }

    *r = b')' as c_char;
    r = r.add(1);
    *r = b'$' as c_char;
    r = r.add(1);

    SET_VARSIZE(
        result as *mut c_char,
        (r as isize - result as isize) as int32,
    );

    result
}

/*
 * similar_to_escape(pattern, escape)
 */
pub unsafe fn similar_to_escape_2(fcinfo: FunctionCallInfo) -> Datum {
    let pat_text: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let esc_text: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let result: *mut text;

    result = similar_escape_internal(pat_text, esc_text);

    PG_RETURN_TEXT_P!(result);
}

/*
 * similar_to_escape(pattern)
 * Inserts a default escape character.
 */
pub unsafe fn similar_to_escape_1(fcinfo: FunctionCallInfo) -> Datum {
    let pat_text: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let result: *mut text;

    result = similar_escape_internal(pat_text, std::ptr::null_mut());

    PG_RETURN_TEXT_P!(result);
}

/*
 * similar_escape(pattern, escape)
 *
 * Legacy function for compatibility with views stored using the
 * pre-v13 expansion of SIMILAR TO.  Unlike the above functions, this
 * is non-strict, which leads to not-per-spec handling of "ESCAPE NULL".
 */
pub unsafe fn similar_escape(fcinfo: FunctionCallInfo) -> Datum {
    let pat_text: *mut text;
    let esc_text: *mut text;
    let result: *mut text;

    /* This function is not strict, so must test explicitly */
    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(fcinfo);
    }
    pat_text = PG_GETARG_TEXT_PP!(fcinfo, 0);

    if PG_ARGISNULL!(fcinfo, 1) {
        esc_text = std::ptr::null_mut(); /* use default escape character */
    } else {
        esc_text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    }

    result = similar_escape_internal(pat_text, esc_text);

    PG_RETURN_TEXT_P!(result);
}

/*
 * regexp_count()
 *		Return the number of matches of a pattern within a string.
 */
pub unsafe fn regexp_count(fcinfo: FunctionCallInfo) -> Datum {
    let str_: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pattern: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut start: c_int = 1;
    let flags: *mut text = PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 3);
    let mut re_flags: pg_re_flags = std::mem::zeroed();
    let matchctx: *mut regexp_matches_ctx;

    /* Collect optional parameters */
    if (PG_NARGS!(fcinfo) as c_int) > 2 {
        start = PG_GETARG_INT32!(fcinfo, 2);
        if start <= 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "start", start)
            );
        }
    }

    /* Determine options */
    parse_re_flags(&raw mut re_flags, flags);
    /* User mustn't specify 'g' */
    if re_flags.glob {
        ereport!(
            ERROR,
            /* translator: %s is a SQL function name */
            errmsg!("{} does not support the \"global\" option", "regexp_count()")
        );
    }
    /* But we find all the matches anyway */
    re_flags.glob = true;

    /* Do the matching */
    matchctx = setup_regexp_matches(
        str_,
        pattern,
        &raw mut re_flags,
        start - 1,
        PG_GET_COLLATION!(fcinfo),
        false, /* can ignore subexprs */
        false,
        false,
    );

    PG_RETURN_INT32!((*matchctx).nmatches);
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_count_no_start(fcinfo: FunctionCallInfo) -> Datum {
    regexp_count(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_count_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    regexp_count(fcinfo)
}

/*
 * regexp_instr()
 *		Return the match's position within the string
 */
pub unsafe fn regexp_instr(fcinfo: FunctionCallInfo) -> Datum {
    let str_: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pattern: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut start: c_int = 1;
    let mut n: c_int = 1;
    let mut endoption: c_int = 0;
    let flags: *mut text = PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 5);
    let mut subexpr: c_int = 0;
    let mut pos: c_int;
    let mut re_flags: pg_re_flags = std::mem::zeroed();
    let matchctx: *mut regexp_matches_ctx;

    /* Collect optional parameters */
    if (PG_NARGS!(fcinfo) as c_int) > 2 {
        start = PG_GETARG_INT32!(fcinfo, 2);
        if start <= 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "start", start)
            );
        }
    }
    if (PG_NARGS!(fcinfo) as c_int) > 3 {
        n = PG_GETARG_INT32!(fcinfo, 3);
        if n <= 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "n", n)
            );
        }
    }
    if (PG_NARGS!(fcinfo) as c_int) > 4 {
        endoption = PG_GETARG_INT32!(fcinfo, 4);
        if endoption != 0 && endoption != 1 {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid value for parameter \"{}\": {}",
                    "endoption",
                    endoption
                )
            );
        }
    }
    if (PG_NARGS!(fcinfo) as c_int) > 6 {
        subexpr = PG_GETARG_INT32!(fcinfo, 6);
        if subexpr < 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "subexpr", subexpr)
            );
        }
    }

    /* Determine options */
    parse_re_flags(&raw mut re_flags, flags);
    /* User mustn't specify 'g' */
    if re_flags.glob {
        ereport!(
            ERROR,
            /* translator: %s is a SQL function name */
            errmsg!("{} does not support the \"global\" option", "regexp_instr()")
        );
    }
    /* But we find all the matches anyway */
    re_flags.glob = true;

    /* Do the matching */
    matchctx = setup_regexp_matches(
        str_,
        pattern,
        &raw mut re_flags,
        start - 1,
        PG_GET_COLLATION!(fcinfo),
        subexpr > 0, /* need submatches? */
        false,
        false,
    );

    /* When n exceeds matches return 0 (includes case of no matches) */
    if n > (*matchctx).nmatches {
        PG_RETURN_INT32!(0);
    }

    /* When subexpr exceeds number of subexpressions return 0 */
    if subexpr > (*matchctx).npatterns {
        PG_RETURN_INT32!(0);
    }

    /* Select the appropriate match position to return */
    pos = (n - 1) * (*matchctx).npatterns;
    if subexpr > 0 {
        pos += subexpr - 1;
    }
    pos *= 2;
    if endoption == 1 {
        pos += 1;
    }

    if *(*matchctx).match_locs.add(pos as usize) >= 0 {
        PG_RETURN_INT32!(*(*matchctx).match_locs.add(pos as usize) + 1);
    } else {
        PG_RETURN_INT32!(0); /* position not identifiable */
    }
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_instr_no_start(fcinfo: FunctionCallInfo) -> Datum {
    regexp_instr(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_instr_no_n(fcinfo: FunctionCallInfo) -> Datum {
    regexp_instr(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_instr_no_endoption(fcinfo: FunctionCallInfo) -> Datum {
    regexp_instr(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_instr_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    regexp_instr(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_instr_no_subexpr(fcinfo: FunctionCallInfo) -> Datum {
    regexp_instr(fcinfo)
}

/*
 * regexp_like()
 *		Test for a pattern match within a string.
 */
pub unsafe fn regexp_like(fcinfo: FunctionCallInfo) -> Datum {
    let str_: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pattern: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let flags: *mut text = PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 2);
    let mut re_flags: pg_re_flags = std::mem::zeroed();

    /* Determine options */
    parse_re_flags(&raw mut re_flags, flags);
    /* User mustn't specify 'g' */
    if re_flags.glob {
        ereport!(
            ERROR,
            /* translator: %s is a SQL function name */
            errmsg!("{} does not support the \"global\" option", "regexp_like()")
        );
    }

    /* Otherwise it's like textregexeq/texticregexeq */
    PG_RETURN_BOOL!(RE_compile_and_execute(
        pattern,
        VARDATA_ANY(str_ as *const c_char),
        VARSIZE_ANY_EXHDR(str_ as *const c_char) as c_int,
        re_flags.cflags,
        PG_GET_COLLATION!(fcinfo),
        0,
        std::ptr::null_mut()
    ));
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_like_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    regexp_like(fcinfo)
}

/*
 * regexp_match()
 *		Return the first substring(s) matching a pattern within a string.
 */
pub unsafe fn regexp_match(fcinfo: FunctionCallInfo) -> Datum {
    let orig_str: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pattern: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let flags: *mut text = PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 2);
    let mut re_flags: pg_re_flags = std::mem::zeroed();
    let matchctx: *mut regexp_matches_ctx;

    /* Determine options */
    parse_re_flags(&raw mut re_flags, flags);
    /* User mustn't specify 'g' */
    if re_flags.glob {
        ereport!(
            ERROR,
            /* translator: %s is a SQL function name */
            errmsg!("{} does not support the \"global\" option", "regexp_match()")
        );
    }

    matchctx = setup_regexp_matches(
        orig_str,
        pattern,
        &raw mut re_flags,
        0,
        PG_GET_COLLATION!(fcinfo),
        true,
        false,
        false,
    );

    if (*matchctx).nmatches == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    Assert!((*matchctx).nmatches == 1);

    /* Create workspace that build_regexp_match_result needs */
    (*matchctx).elems =
        palloc(std::mem::size_of::<Datum>() * (*matchctx).npatterns as usize) as *mut Datum;
    (*matchctx).nulls =
        palloc(std::mem::size_of::<bool>() * (*matchctx).npatterns as usize) as *mut bool;

    PG_RETURN_DATUM!(PointerGetDatum(
        build_regexp_match_result(matchctx) as *const c_void
    ))
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_match_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    regexp_match(fcinfo)
}

/*
 * regexp_matches()
 *		Return a table of all matches of a pattern within a string.
 */
pub unsafe fn regexp_matches(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let matchctx: *mut regexp_matches_ctx;

    if SRF_IS_FIRSTCALL!(fcinfo) {
        let pattern: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
        let flags: *mut text = PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 2);
        let mut re_flags: pg_re_flags = std::mem::zeroed();
        let oldcontext: MemoryContext;

        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /* Determine options */
        parse_re_flags(&raw mut re_flags, flags);

        /* be sure to copy the input string into the multi-call ctx */
        let matchctx = setup_regexp_matches(
            PG_GETARG_TEXT_P_COPY!(fcinfo, 0),
            pattern,
            &raw mut re_flags,
            0,
            PG_GET_COLLATION!(fcinfo),
            true,
            false,
            false,
        );

        /* Pre-create workspace that build_regexp_match_result needs */
        (*matchctx).elems =
            palloc(std::mem::size_of::<Datum>() * (*matchctx).npatterns as usize) as *mut Datum;
        (*matchctx).nulls =
            palloc(std::mem::size_of::<bool>() * (*matchctx).npatterns as usize) as *mut bool;

        MemoryContextSwitchTo(oldcontext);
        (*funcctx).user_fctx = matchctx as *mut c_void;
    }

    funcctx = SRF_PERCALL_SETUP!(fcinfo);
    matchctx = (*funcctx).user_fctx as *mut regexp_matches_ctx;

    if (*matchctx).next_match < (*matchctx).nmatches {
        let result_ary: *mut ArrayType;

        result_ary = build_regexp_match_result(matchctx);
        (*matchctx).next_match += 1;
        SRF_RETURN_NEXT!(fcinfo, funcctx, PointerGetDatum(result_ary as *const c_void));
    }

    SRF_RETURN_DONE!(fcinfo, funcctx);
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_matches_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    regexp_matches(fcinfo)
}

/*
 * setup_regexp_matches --- do the initial matching for regexp_match,
 *		regexp_split, and related functions
 *
 * To avoid having to re-find the compiled pattern on each call, we do
 * all the matching in one swoop.  The returned regexp_matches_ctx contains
 * the locations of all the substrings matching the pattern.
 *
 * start_search: the character (not byte) offset in orig_str at which to
 * begin the search.  Returned positions are relative to orig_str anyway.
 * use_subpatterns: collect data about matches to parenthesized subexpressions.
 * ignore_degenerate: ignore zero-length matches.
 * fetching_unmatched: caller wants to fetch unmatched substrings.
 *
 * We don't currently assume that fetching_unmatched is exclusive of fetching
 * the matched text too; if it's set, the conversion buffer is large enough to
 * fetch any single matched or unmatched string, but not any larger
 * substring.  (In practice, when splitting the matches are usually small
 * anyway, and it didn't seem worth complicating the code further.)
 */
unsafe fn setup_regexp_matches(
    orig_str: *mut text,
    pattern: *mut text,
    re_flags: *mut pg_re_flags,
    mut start_search: c_int,
    collation: Oid,
    mut use_subpatterns: bool,
    ignore_degenerate: bool,
    fetching_unmatched: bool,
) -> *mut regexp_matches_ctx {
    let matchctx: *mut regexp_matches_ctx =
        palloc0(std::mem::size_of::<regexp_matches_ctx>()) as *mut regexp_matches_ctx;
    let eml: c_int = pg_database_encoding_max_length();
    let orig_len: c_int;
    let wide_str: *mut pg_wchar;
    let wide_len: c_int;
    let mut cflags: c_int;
    let cpattern: *mut regex_t;
    let pmatch: *mut regmatch_t;
    let pmatch_len: c_int;
    let mut array_len: c_int;
    let mut array_idx: c_int;
    let mut prev_match_end: c_int;
    let mut prev_valid_match_end: c_int;
    let mut maxlen: c_int = 0; /* largest fetch length in characters */

    /* save original string --- we'll extract result substrings from it */
    (*matchctx).orig_str = orig_str;

    /* convert string to pg_wchar form for matching */
    orig_len = VARSIZE_ANY_EXHDR(orig_str as *const c_char) as c_int;
    wide_str = palloc(std::mem::size_of::<pg_wchar>() * ((orig_len + 1) as usize)) as *mut pg_wchar;
    wide_len = pg_mb2wchar_with_len(VARDATA_ANY(orig_str as *const c_char), wide_str, orig_len);

    /* set up the compiled pattern */
    cflags = (*re_flags).cflags;
    if !use_subpatterns {
        cflags |= REG_NOSUB;
    }
    cpattern = RE_compile_and_cache(pattern, cflags, collation);

    /* do we want to remember subpatterns? */
    if use_subpatterns && (*cpattern).re_nsub > 0 {
        (*matchctx).npatterns = (*cpattern).re_nsub as c_int;
        pmatch_len = (*cpattern).re_nsub as c_int + 1;
    } else {
        use_subpatterns = false;
        (*matchctx).npatterns = 1;
        pmatch_len = 1;
    }

    /* temporary output space for RE package */
    pmatch = palloc(std::mem::size_of::<regmatch_t>() * pmatch_len as usize) as *mut regmatch_t;

    /*
     * the real output space (grown dynamically if needed)
     *
     * use values 2^n-1, not 2^n, so that we hit the limit at 2^28-1 rather
     * than at 2^27
     */
    array_len = if (*re_flags).glob { 255 } else { 31 };
    (*matchctx).match_locs = palloc(std::mem::size_of::<c_int>() * array_len as usize) as *mut c_int;
    array_idx = 0;

    /* search for the pattern, perhaps repeatedly */
    prev_match_end = 0;
    prev_valid_match_end = 0;
    while RE_wchar_execute(cpattern, wide_str, wide_len, start_search, pmatch_len, pmatch) {
        /*
         * If requested, ignore degenerate matches, which are zero-length
         * matches occurring at the start or end of a string or just after a
         * previous match.
         */
        if !ignore_degenerate
            || ((*pmatch.add(0)).rm_so < wide_len as crate::regex::regex::pg_regoff_t
                && (*pmatch.add(0)).rm_eo > prev_match_end as crate::regex::regex::pg_regoff_t)
        {
            /* enlarge output space if needed */
            while array_idx + (*matchctx).npatterns * 2 + 1 > array_len {
                array_len += array_len + 1; /* 2^n-1 => 2^(n+1)-1 */
                if array_len as Size > MaxAllocSize / std::mem::size_of::<c_int>() {
                    ereport!(ERROR, errmsg!("too many regular expression matches"));
                }
                (*matchctx).match_locs = repalloc(
                    (*matchctx).match_locs as *mut c_void,
                    std::mem::size_of::<c_int>() * array_len as usize,
                ) as *mut c_int;
            }

            /* save this match's locations */
            if use_subpatterns {
                let mut i: c_int = 1;

                while i <= (*matchctx).npatterns {
                    let so: c_int = (*pmatch.add(i as usize)).rm_so as c_int;
                    let eo: c_int = (*pmatch.add(i as usize)).rm_eo as c_int;

                    *(*matchctx).match_locs.add(array_idx as usize) = so;
                    array_idx += 1;
                    *(*matchctx).match_locs.add(array_idx as usize) = eo;
                    array_idx += 1;
                    if so >= 0 && eo >= 0 && (eo - so) > maxlen {
                        maxlen = eo - so;
                    }
                    i += 1;
                }
            } else {
                let so: c_int = (*pmatch.add(0)).rm_so as c_int;
                let eo: c_int = (*pmatch.add(0)).rm_eo as c_int;

                *(*matchctx).match_locs.add(array_idx as usize) = so;
                array_idx += 1;
                *(*matchctx).match_locs.add(array_idx as usize) = eo;
                array_idx += 1;
                if so >= 0 && eo >= 0 && (eo - so) > maxlen {
                    maxlen = eo - so;
                }
            }
            (*matchctx).nmatches += 1;

            /*
             * check length of unmatched portion between end of previous valid
             * (nondegenerate, or degenerate but not ignored) match and start
             * of current one
             */
            if fetching_unmatched
                && (*pmatch.add(0)).rm_so >= 0
                && ((*pmatch.add(0)).rm_so as c_int - prev_valid_match_end) > maxlen
            {
                maxlen = (*pmatch.add(0)).rm_so as c_int - prev_valid_match_end;
            }
            prev_valid_match_end = (*pmatch.add(0)).rm_eo as c_int;
        }
        prev_match_end = (*pmatch.add(0)).rm_eo as c_int;

        /* if not glob, stop after one match */
        if !(*re_flags).glob {
            break;
        }

        /*
         * Advance search position.  Normally we start the next search at the
         * end of the previous match; but if the match was of zero length, we
         * have to advance by one character, or we'd just find the same match
         * again.
         */
        start_search = prev_match_end;
        if (*pmatch.add(0)).rm_so == (*pmatch.add(0)).rm_eo {
            start_search += 1;
        }
        if start_search > wide_len {
            break;
        }
    }

    /*
     * check length of unmatched portion between end of last match and end of
     * input string
     */
    if fetching_unmatched && (wide_len - prev_valid_match_end) > maxlen {
        maxlen = wide_len - prev_valid_match_end;
    }

    /*
     * Keep a note of the end position of the string for the benefit of
     * splitting code.
     */
    *(*matchctx).match_locs.add(array_idx as usize) = wide_len;

    if eml > 1 {
        let maxsiz: int64 = eml as int64 * maxlen as int64;
        let conv_bufsiz: c_int;

        /*
         * Make the conversion buffer large enough for any substring of
         * interest.
         *
         * Worst case: assume we need the maximum size (maxlen*eml), but take
         * advantage of the fact that the original string length in bytes is
         * an upper bound on the byte length of any fetched substring (and we
         * know that len+1 is safe to allocate because the varlena header is
         * longer than 1 byte).
         */
        if maxsiz > orig_len as int64 {
            conv_bufsiz = orig_len + 1;
        } else {
            conv_bufsiz = maxsiz as c_int + 1; /* safe since maxsiz < 2^30 */
        }

        (*matchctx).conv_buf = palloc(conv_bufsiz as Size) as *mut c_char;
        (*matchctx).conv_bufsiz = conv_bufsiz;
        (*matchctx).wide_str = wide_str;
    } else {
        /* No need to keep the wide string if we're in a single-byte charset. */
        pfree(wide_str as *mut c_void);
        (*matchctx).wide_str = std::ptr::null_mut();
        (*matchctx).conv_buf = std::ptr::null_mut();
        (*matchctx).conv_bufsiz = 0;
    }

    /* Clean up temp storage */
    pfree(pmatch as *mut c_void);

    matchctx
}

/*
 * build_regexp_match_result - build output array for current match
 */
unsafe fn build_regexp_match_result(matchctx: *mut regexp_matches_ctx) -> *mut ArrayType {
    let buf: *mut c_char = (*matchctx).conv_buf;
    let elems: *mut Datum = (*matchctx).elems;
    let nulls: *mut bool = (*matchctx).nulls;
    let mut dims: [c_int; 1] = [0; 1];
    let mut lbs: [c_int; 1] = [0; 1];
    let mut loc: c_int;
    let mut i: c_int;

    /* Extract matching substrings from the original string */
    loc = (*matchctx).next_match * (*matchctx).npatterns * 2;
    i = 0;
    while i < (*matchctx).npatterns {
        let so: c_int = *(*matchctx).match_locs.add(loc as usize);
        loc += 1;
        let eo: c_int = *(*matchctx).match_locs.add(loc as usize);
        loc += 1;

        if so < 0 || eo < 0 {
            *elems.add(i as usize) = 0 as Datum;
            *nulls.add(i as usize) = true;
        } else if !buf.is_null() {
            let len: c_int = pg_wchar2mb_with_len((*matchctx).wide_str.add(so as usize), buf, eo - so);

            Assert!(len < (*matchctx).conv_bufsiz);
            *elems.add(i as usize) =
                PointerGetDatum(cstring_to_text_with_len(buf, len) as *const c_void);
            *nulls.add(i as usize) = false;
        } else {
            *elems.add(i as usize) = DirectFunctionCall3!(
                text_substr,
                PointerGetDatum((*matchctx).orig_str as *const c_void),
                Int32GetDatum(so + 1),
                Int32GetDatum(eo - so)
            );
            *nulls.add(i as usize) = false;
        }
        i += 1;
    }

    /* And form an array */
    dims[0] = (*matchctx).npatterns;
    lbs[0] = 1;
    /* XXX: this hardcodes assumptions about the text type */
    construct_md_array(
        elems,
        nulls,
        1,
        dims.as_mut_ptr(),
        lbs.as_mut_ptr(),
        TEXTOID,
        -1,
        false,
        TYPALIGN_INT,
    )
}

/*
 * regexp_split_to_table()
 *		Split the string at matches of the pattern, returning the
 *		split-out substrings as a table.
 */
pub unsafe fn regexp_split_to_table(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let splitctx: *mut regexp_matches_ctx;

    if SRF_IS_FIRSTCALL!(fcinfo) {
        let pattern: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
        let flags: *mut text = PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 2);
        let mut re_flags: pg_re_flags = std::mem::zeroed();
        let oldcontext: MemoryContext;

        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /* Determine options */
        parse_re_flags(&raw mut re_flags, flags);
        /* User mustn't specify 'g' */
        if re_flags.glob {
            ereport!(
                ERROR,
                /* translator: %s is a SQL function name */
                errmsg!(
                    "{} does not support the \"global\" option",
                    "regexp_split_to_table()"
                )
            );
        }
        /* But we find all the matches anyway */
        re_flags.glob = true;

        /* be sure to copy the input string into the multi-call ctx */
        let splitctx = setup_regexp_matches(
            PG_GETARG_TEXT_P_COPY!(fcinfo, 0),
            pattern,
            &raw mut re_flags,
            0,
            PG_GET_COLLATION!(fcinfo),
            false,
            true,
            true,
        );

        MemoryContextSwitchTo(oldcontext);
        (*funcctx).user_fctx = splitctx as *mut c_void;
    }

    funcctx = SRF_PERCALL_SETUP!(fcinfo);
    splitctx = (*funcctx).user_fctx as *mut regexp_matches_ctx;

    if (*splitctx).next_match <= (*splitctx).nmatches {
        let result: Datum = build_regexp_split_result(splitctx);

        (*splitctx).next_match += 1;
        SRF_RETURN_NEXT!(fcinfo, funcctx, result);
    }

    SRF_RETURN_DONE!(fcinfo, funcctx);
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_split_to_table_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    regexp_split_to_table(fcinfo)
}

/*
 * regexp_split_to_array()
 *		Split the string at matches of the pattern, returning the
 *		split-out substrings as an array.
 */
pub unsafe fn regexp_split_to_array(fcinfo: FunctionCallInfo) -> Datum {
    let mut astate: *mut ArrayBuildState = std::ptr::null_mut();
    let mut re_flags: pg_re_flags = std::mem::zeroed();
    let splitctx: *mut regexp_matches_ctx;

    /* Determine options */
    parse_re_flags(&raw mut re_flags, PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 2));
    /* User mustn't specify 'g' */
    if re_flags.glob {
        ereport!(
            ERROR,
            /* translator: %s is a SQL function name */
            errmsg!(
                "{} does not support the \"global\" option",
                "regexp_split_to_array()"
            )
        );
    }
    /* But we find all the matches anyway */
    re_flags.glob = true;

    splitctx = setup_regexp_matches(
        PG_GETARG_TEXT_PP!(fcinfo, 0),
        PG_GETARG_TEXT_PP!(fcinfo, 1),
        &raw mut re_flags,
        0,
        PG_GET_COLLATION!(fcinfo),
        false,
        true,
        true,
    );

    while (*splitctx).next_match <= (*splitctx).nmatches {
        astate = accumArrayResult(
            astate,
            build_regexp_split_result(splitctx),
            false,
            TEXTOID,
            CurrentMemoryContext,
        );
        (*splitctx).next_match += 1;
    }

    PG_RETURN_DATUM!(makeArrayResult(astate, CurrentMemoryContext))
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_split_to_array_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    regexp_split_to_array(fcinfo)
}

/*
 * build_regexp_split_result - build output string for current match
 *
 * We return the string between the current match and the previous one,
 * or the string after the last match when next_match == nmatches.
 */
unsafe fn build_regexp_split_result(splitctx: *mut regexp_matches_ctx) -> Datum {
    let buf: *mut c_char = (*splitctx).conv_buf;
    let startpos: c_int;
    let endpos: c_int;

    if (*splitctx).next_match > 0 {
        startpos = *(*splitctx).match_locs.add(((*splitctx).next_match * 2 - 1) as usize);
    } else {
        startpos = 0;
    }
    if startpos < 0 {
        elog!(ERROR, "invalid match ending position");
    }

    endpos = *(*splitctx).match_locs.add(((*splitctx).next_match * 2) as usize);
    if endpos < startpos {
        elog!(ERROR, "invalid match starting position");
    }

    if !buf.is_null() {
        let len: c_int;

        len = pg_wchar2mb_with_len(
            (*splitctx).wide_str.add(startpos as usize),
            buf,
            endpos - startpos,
        );
        Assert!(len < (*splitctx).conv_bufsiz);
        PointerGetDatum(cstring_to_text_with_len(buf, len) as *const c_void)
    } else {
        DirectFunctionCall3!(
            text_substr,
            PointerGetDatum((*splitctx).orig_str as *const c_void),
            Int32GetDatum(startpos + 1),
            Int32GetDatum(endpos - startpos)
        )
    }
}

/*
 * regexp_substr()
 *		Return the substring that matches a regular expression pattern
 */
pub unsafe fn regexp_substr(fcinfo: FunctionCallInfo) -> Datum {
    let str_: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let pattern: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut start: c_int = 1;
    let mut n: c_int = 1;
    let flags: *mut text = PG_GETARG_TEXT_PP_IF_EXISTS!(fcinfo, 4);
    let mut subexpr: c_int = 0;
    let so: c_int;
    let eo: c_int;
    let mut pos: c_int;
    let mut re_flags: pg_re_flags = std::mem::zeroed();
    let matchctx: *mut regexp_matches_ctx;

    /* Collect optional parameters */
    if (PG_NARGS!(fcinfo) as c_int) > 2 {
        start = PG_GETARG_INT32!(fcinfo, 2);
        if start <= 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "start", start)
            );
        }
    }
    if (PG_NARGS!(fcinfo) as c_int) > 3 {
        n = PG_GETARG_INT32!(fcinfo, 3);
        if n <= 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "n", n)
            );
        }
    }
    if (PG_NARGS!(fcinfo) as c_int) > 5 {
        subexpr = PG_GETARG_INT32!(fcinfo, 5);
        if subexpr < 0 {
            ereport!(
                ERROR,
                errmsg!("invalid value for parameter \"{}\": {}", "subexpr", subexpr)
            );
        }
    }

    /* Determine options */
    parse_re_flags(&raw mut re_flags, flags);
    /* User mustn't specify 'g' */
    if re_flags.glob {
        ereport!(
            ERROR,
            /* translator: %s is a SQL function name */
            errmsg!("{} does not support the \"global\" option", "regexp_substr()")
        );
    }
    /* But we find all the matches anyway */
    re_flags.glob = true;

    /* Do the matching */
    matchctx = setup_regexp_matches(
        str_,
        pattern,
        &raw mut re_flags,
        start - 1,
        PG_GET_COLLATION!(fcinfo),
        subexpr > 0, /* need submatches? */
        false,
        false,
    );

    /* When n exceeds matches return NULL (includes case of no matches) */
    if n > (*matchctx).nmatches {
        PG_RETURN_NULL!(fcinfo);
    }

    /* When subexpr exceeds number of subexpressions return NULL */
    if subexpr > (*matchctx).npatterns {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Select the appropriate match position to return */
    pos = (n - 1) * (*matchctx).npatterns;
    if subexpr > 0 {
        pos += subexpr - 1;
    }
    pos *= 2;
    so = *(*matchctx).match_locs.add(pos as usize);
    eo = *(*matchctx).match_locs.add((pos + 1) as usize);

    if so < 0 || eo < 0 {
        PG_RETURN_NULL!(fcinfo); /* unidentifiable location */
    }

    PG_RETURN_DATUM!(DirectFunctionCall3!(
        text_substr,
        PointerGetDatum((*matchctx).orig_str as *const c_void),
        Int32GetDatum(so + 1),
        Int32GetDatum(eo - so)
    ))
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_substr_no_start(fcinfo: FunctionCallInfo) -> Datum {
    regexp_substr(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_substr_no_n(fcinfo: FunctionCallInfo) -> Datum {
    regexp_substr(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_substr_no_flags(fcinfo: FunctionCallInfo) -> Datum {
    regexp_substr(fcinfo)
}

/* This is separate to keep the opr_sanity regression test from complaining */
pub unsafe fn regexp_substr_no_subexpr(fcinfo: FunctionCallInfo) -> Datum {
    regexp_substr(fcinfo)
}

/*
 * regexp_fixed_prefix - extract fixed prefix, if any, for a regexp
 *
 * The result is NULL if there is no fixed prefix, else a palloc'd string.
 * If it is an exact match, not just a prefix, *exact is returned as true.
 */
pub unsafe fn regexp_fixed_prefix(
    text_re: *mut text,
    case_insensitive: bool,
    collation: Oid,
    exact: *mut bool,
) -> *mut c_char {
    let result: *mut c_char;
    let re: *mut regex_t;
    let mut cflags: c_int;
    let re_result: c_int;
    let mut str: *mut pg_wchar = std::ptr::null_mut();
    let mut slen: Size = 0;
    let maxlen: Size;
    let mut errMsg: [c_char; 100] = [0; 100];

    *exact = false; /* default result */

    /* Compile RE */
    cflags = REG_ADVANCED;
    if case_insensitive {
        cflags |= REG_ICASE;
    }

    re = RE_compile_and_cache(text_re, cflags | REG_NOSUB, collation);

    /* Examine it to see if there's a fixed prefix */
    re_result = pg_regprefix(re, &raw mut str, &raw mut slen);

    match re_result {
        REG_NOMATCH => {
            return std::ptr::null_mut();
        }

        REG_PREFIX => {
            /* continue with wchar conversion */
        }

        REG_EXACT => {
            *exact = true;
            /* continue with wchar conversion */
        }

        _ => {
            /* re failed??? */
            pg_regerror(
                re_result,
                re,
                errMsg.as_mut_ptr(),
                std::mem::size_of_val(&errMsg) as Size,
            );
            ereport!(
                ERROR,
                errmsg!(
                    "regular expression failed: {}",
                    std::ffi::CStr::from_ptr(errMsg.as_ptr()).to_string_lossy()
                )
            );
        }
    }

    /* Convert pg_wchar result back to database encoding */
    maxlen = pg_database_encoding_max_length() as Size * slen + 1;
    result = palloc(maxlen) as *mut c_char;
    slen = pg_wchar2mb_with_len(str, result, slen as c_int) as Size;
    Assert!(slen < maxlen);

    pfree(str as *mut c_void);

    result
}

//
// ---------------------------------------------------------------------------
// Local stubs for symbols not yet ported elsewhere in the tree.
// ---------------------------------------------------------------------------
//

// libc memcmp/memcpy/memmove -- raw byte ops used verbatim from the C source.
unsafe fn libc_memcmp(a: *const c_char, b: *const c_char, n: usize) -> c_int {
    let sa = core::slice::from_raw_parts(a as *const u8, n);
    let sb = core::slice::from_raw_parts(b as *const u8, n);
    match sa.cmp(sb) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}
unsafe fn libc_memcpy(dst: *mut c_char, src: *const c_char, n: usize) {
    core::ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, n);
}
unsafe fn libc_memmove(dst: *mut c_void, src: *const c_void, n: usize) {
    core::ptr::copy(src as *const u8, dst as *mut u8, n);
}

// strlen(3) -- C standard library.
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

// pg_mblen_range(p, end) returns the byte length of the multibyte char at p.
// The C source builds a "%.*s" slice of that many bytes for error messages;
// mblen_slice reproduces that lossy slice as a String for errmsg!().
unsafe fn mblen_slice(p: *const c_char, end: *const c_char) -> std::string::String {
    let n = pg_mblen_range(p, end) as usize;
    let bytes = core::slice::from_raw_parts(p as *const u8, n);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

// text_substr(fcinfo) (utils/adt/varlena.c) -- not yet ported; stubbed locally.
unsafe fn text_substr(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): real text_substr lives in utils/adt/varlena.rs
}

// replace_text_regexp(...) (utils/adt/varlena.c) -- not yet ported; stubbed locally.
// TODO(pg-port): real replace_text_regexp lives in utils/adt/varlena.rs
unsafe fn replace_text_regexp(
    _src_text: *mut text,
    _pattern: *mut text,
    _replace_text: *mut text,
    _cflags: c_int,
    _collation: Oid,
    _search_start: c_int,
    _n: c_int,
) -> *mut text {
    unimplemented!() // TODO(pg-port): real replace_text_regexp lives in utils/adt/varlena.rs
}

// ArrayBuildState (utils/adt/arrayfuncs.c) -- not yet ported; opaque stub.
// TODO(pg-port): real ArrayBuildState lives in utils/adt/array.rs
type ArrayBuildState = c_void;

// construct_md_array(...) (utils/adt/arrayfuncs.c) -- not yet ported; stubbed locally.
// TODO(pg-port): real construct_md_array lives in utils/adt/arrayfuncs.rs
unsafe fn construct_md_array(
    _elems: *mut Datum,
    _nulls: *mut bool,
    _ndims: c_int,
    _dims: *mut c_int,
    _lbs: *mut c_int,
    _elmtype: Oid,
    _elmlen: c_int,
    _elmbyval: bool,
    _elmalign: c_char,
) -> *mut ArrayType {
    unimplemented!() // TODO(pg-port): real construct_md_array lives in utils/adt/arrayfuncs.rs
}

// accumArrayResult(...) (utils/adt/arrayfuncs.c) -- not yet ported; stubbed locally.
// TODO(pg-port): real accumArrayResult lives in utils/adt/arrayfuncs.rs
unsafe fn accumArrayResult(
    _astate: *mut ArrayBuildState,
    _dvalue: Datum,
    _disnull: bool,
    _element_type: Oid,
    _rcontext: MemoryContext,
) -> *mut ArrayBuildState {
    unimplemented!() // TODO(pg-port): real accumArrayResult lives in utils/adt/arrayfuncs.rs
}

// makeArrayResult(...) (utils/adt/arrayfuncs.c) -- not yet ported; stubbed locally.
// TODO(pg-port): real makeArrayResult lives in utils/adt/arrayfuncs.rs
unsafe fn makeArrayResult(_astate: *mut ArrayBuildState, _rcontext: MemoryContext) -> Datum {
    unimplemented!() // TODO(pg-port): real makeArrayResult lives in utils/adt/arrayfuncs.rs
}

// FuncCallContext (funcapi.h) -- minimal layout matching the fields used here.
// TODO(pg-port): real FuncCallContext lives in utils/fmgr/funcapi.rs
#[repr(C)]
struct FuncCallContext {
    call_cntr: u64,
    max_calls: u64,
    user_fctx: *mut c_void,
    attinmeta: *mut c_void,
    multi_call_memory_ctx: MemoryContext,
    tuple_desc: *mut c_void,
}

// SRF support (funcapi.h) -- not yet ported; stubbed locally.
unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_next(
    _fcinfo: FunctionCallInfo,
    _fctx: *mut FuncCallContext,
    _result: Datum,
) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_done(_fcinfo: FunctionCallInfo, _fctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}

// The SRF_* macros are defined just below the imports (textual scope).
