//! regex/regex.h - POSIX-compatible regular expression interface types and prototypes.

use std::ffi::{c_char, c_int, c_long};

use crate::c::{text, Size};
use crate::mb::pg_wchar::pg_wchar;
use crate::postgres_ext::Oid;

/*
 * regoff_t has to be large enough to hold either off_t or ssize_t,
 * and must be signed; it's only a guess that long is suitable.
 */
pub type pg_regoff_t = c_long;

/* the biggie, a compiled RE (or rather, a front end to same) */
#[repr(C)]
pub struct pg_regex_t {
    pub re_magic: c_int,    /* magic number */
    pub re_nsub: Size,      /* number of subexpressions */
    pub re_info: c_long,    /* bitmask of the following flags: */
    pub re_csize: c_int,    /* sizeof(character) */
    pub re_endp: *mut c_char, /* backward compatibility kludge */
    pub re_collation: Oid,  /* Collation that defines LC_CTYPE behavior */
    /* the rest is opaque pointers to hidden innards */
    pub re_guts: *mut c_char, /* `char *' is more portable than `void *' */
    pub re_fns: *mut c_char,
}

/* re_info bitmask flags */
pub const REG_UBACKREF: c_long = 0o000001; /* has back-reference (\n) */
pub const REG_ULOOKAROUND: c_long = 0o000002; /* has lookahead/lookbehind constraint */
pub const REG_UBOUNDS: c_long = 0o000004; /* has bounded quantifier ({m,n}) */
pub const REG_UBRACES: c_long = 0o000010; /* has { that doesn't begin a quantifier */
pub const REG_UBSALNUM: c_long = 0o000020; /* has backslash-alphanumeric in non-ARE */
pub const REG_UPBOTCH: c_long = 0o000040; /* has unmatched right paren in ERE */
pub const REG_UBBS: c_long = 0o000100; /* has backslash within bracket expr */
pub const REG_UNONPOSIX: c_long = 0o000200; /* has any construct that extends POSIX */
pub const REG_UUNSPEC: c_long = 0o000400; /* has any case disallowed by POSIX */
pub const REG_UUNPORT: c_long = 0o001000; /* has numeric character code dependency */
pub const REG_ULOCALE: c_long = 0o002000; /* has locale dependency */
pub const REG_UEMPTYMATCH: c_long = 0o004000; /* can match a zero-length string */
pub const REG_UIMPOSSIBLE: c_long = 0o010000; /* provably cannot match anything */
pub const REG_USHORTEST: c_long = 0o020000; /* has non-greedy quantifier */

/* result reporting (may acquire more fields later) */
#[repr(C)]
pub struct pg_regmatch_t {
    pub rm_so: pg_regoff_t, /* start of substring */
    pub rm_eo: pg_regoff_t, /* end of substring */
}

/* supplementary control and reporting */
#[repr(C)]
pub struct rm_detail_t {
    pub rm_extend: pg_regmatch_t, /* see REG_EXPECT */
}

/*
 * regex compilation flags
 */
pub const REG_BASIC: c_int = 0o000000; /* BREs (convenience) */
pub const REG_EXTENDED: c_int = 0o000001; /* EREs */
pub const REG_ADVF: c_int = 0o000002; /* advanced features in EREs */
pub const REG_ADVANCED: c_int = 0o000003; /* AREs (which are also EREs) */
pub const REG_QUOTE: c_int = 0o000004; /* no special characters, none */
pub const REG_NOSPEC: c_int = REG_QUOTE; /* historical synonym */
pub const REG_ICASE: c_int = 0o000010; /* ignore case */
pub const REG_NOSUB: c_int = 0o000020; /* caller doesn't need subexpr match data */
pub const REG_EXPANDED: c_int = 0o000040; /* expanded format, white space & comments */
pub const REG_NLSTOP: c_int = 0o000100; /* \n doesn't match . or [^ ] */
pub const REG_NLANCH: c_int = 0o000200; /* ^ matches after \n, $ before */
pub const REG_NEWLINE: c_int = 0o000300; /* newlines are line terminators */
pub const REG_PEND: c_int = 0o000400; /* ugh -- backward-compatibility hack */
pub const REG_EXPECT: c_int = 0o001000; /* report details on partial/limited matches */
pub const REG_BOSONLY: c_int = 0o002000; /* temporary kludge for BOS-only matches */
pub const REG_DUMP: c_int = 0o004000; /* none of your business :-) */
pub const REG_FAKE: c_int = 0o010000; /* none of your business :-) */
pub const REG_PROGRESS: c_int = 0o020000; /* none of your business :-) */

/*
 * regex execution flags
 */
pub const REG_NOTBOL: c_int = 0o001; /* BOS is not BOL */
pub const REG_NOTEOL: c_int = 0o002; /* EOS is not EOL */
pub const REG_STARTEND: c_int = 0o004; /* backward compatibility kludge */
pub const REG_FTRACE: c_int = 0o010; /* none of your business */
pub const REG_MTRACE: c_int = 0o020; /* none of your business */
pub const REG_SMALL: c_int = 0o040; /* none of your business */

/*
 * error reporting
 */
pub const REG_OKAY: c_int = 0; /* no errors detected */
pub const REG_NOMATCH: c_int = 1; /* failed to match */
pub const REG_BADPAT: c_int = 2; /* invalid regexp */
pub const REG_ECOLLATE: c_int = 3; /* invalid collating element */
pub const REG_ECTYPE: c_int = 4; /* invalid character class */
pub const REG_EESCAPE: c_int = 5; /* invalid escape \ sequence */
pub const REG_ESUBREG: c_int = 6; /* invalid backreference number */
pub const REG_EBRACK: c_int = 7; /* brackets [] not balanced */
pub const REG_EPAREN: c_int = 8; /* parentheses () not balanced */
pub const REG_EBRACE: c_int = 9; /* braces {} not balanced */
pub const REG_BADBR: c_int = 10; /* invalid repetition count(s) */
pub const REG_ERANGE: c_int = 11; /* invalid character range */
pub const REG_ESPACE: c_int = 12; /* out of memory */
pub const REG_BADRPT: c_int = 13; /* quantifier operand invalid */
pub const REG_ASSERT: c_int = 15; /* "can't happen" -- you found a bug */
pub const REG_INVARG: c_int = 16; /* invalid argument to regex function */
pub const REG_MIXED: c_int = 17; /* character widths of regex and string differ */
pub const REG_BADOPT: c_int = 18; /* invalid embedded option */
pub const REG_ETOOBIG: c_int = 19; /* regular expression is too complex */
pub const REG_ECOLORS: c_int = 20; /* too many colors */
/* two specials for debugging and testing */
pub const REG_ATOI: c_int = 101; /* convert error-code name to number */
pub const REG_ITOA: c_int = 102; /* convert error-code number to name */
/* non-error result codes for pg_regprefix */
pub const REG_PREFIX: c_int = -1; /* identified a common prefix */
pub const REG_EXACT: c_int = -2; /* identified an exact match */

/* Redirect the standard typenames to our typenames. */
pub type regoff_t = pg_regoff_t;
pub type regex_t = pg_regex_t;
pub type regmatch_t = pg_regmatch_t;

/*
 * the prototypes for exported functions
 */

/* regcomp.c */
pub unsafe fn pg_regcomp(
    re: *mut regex_t,
    string: *const pg_wchar,
    len: Size,
    flags: c_int,
    collation: Oid,
) -> c_int {
    crate::regex::regcomp::pg_regcomp(re, string as _, len as _, flags, collation)
}

pub unsafe fn pg_regexec(
    re: *mut regex_t,
    string: *const pg_wchar,
    len: Size,
    search_start: Size,
    details: *mut rm_detail_t,
    nmatch: Size,
    pmatch: *mut regmatch_t,
    flags: c_int,
) -> c_int {
    crate::regex::regexec::pg_regexec(
        re,
        string as _,
        len as _,
        search_start as _,
        details as _,
        nmatch as _,
        pmatch as _,
        flags,
    )
}

pub unsafe fn pg_regprefix(
    re: *mut regex_t,
    string: *mut *mut pg_wchar,
    slength: *mut Size,
) -> c_int {
    crate::regex::regprefix::pg_regprefix(re, string as _, slength as _)
}

pub unsafe fn pg_regfree(re: *mut regex_t) {
    crate::regex::regfree::pg_regfree(re)
}

pub unsafe fn pg_regerror(
    errcode: c_int,
    preg: *const regex_t,
    errbuf: *mut c_char,
    errbuf_size: Size,
) -> Size {
    crate::regex::regerror::pg_regerror(errcode, preg as _, errbuf, errbuf_size)
}

/* regexp.c */
pub unsafe fn RE_compile_and_cache(
    text_re: *mut text,
    cflags: c_int,
    collation: Oid,
) -> *mut regex_t {
    crate::utils::adt::regexp::RE_compile_and_cache(text_re, cflags, collation)
}

pub unsafe fn RE_compile_and_execute(
    text_re: *mut text,
    dat: *mut c_char,
    dat_len: c_int,
    cflags: c_int,
    collation: Oid,
    nmatch: c_int,
    pmatch: *mut regmatch_t,
) -> bool {
    crate::utils::adt::regexp::RE_compile_and_execute(
        text_re, dat, dat_len, cflags, collation, nmatch, pmatch as _,
    )
}
