//! Translated from PostgreSQL src/include/regex/regex.h
//
// Vendored Henry Spencer regex engine (hand-written, NOT generated). Server
// encoding is UTF-8. COMPAT-SENSITIVE: SQL `~`/`SIMILAR TO` semantics differ
// from the `regex` crate, so this engine's behavior must be preserved; only the
// type/flag surface is translated here. The system <regex.h> clash handling and
// the `regoff_t`/`regex_t`/`regmatch_t` redirect #defines are C-only and dropped.
#![allow(clippy::needless_pass_by_value, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use crate::c::text;
use crate::mb::pg_wchar::pg_wchar;
use crate::postgres_ext::Oid;

/// C: `typedef long pg_regoff_t;` - signed, holds off_t/ssize_t.
pub type pg_regoff_t = isize;

use bitflags::bitflags;

bitflags! {
    /// `info` bitmask (the REG_U* flags recording features of a compiled RE).
    /// OR-able single bits. (C octal constants.)
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReInfo: i64 {
        const UBACKREF     = 0o000001; // has back-reference (\n)
        const ULOOKAROUND  = 0o000002; // has lookahead/lookbehind
        const UBOUNDS      = 0o000004; // has bounded quantifier ({m,n})
        const UBRACES      = 0o000010; // has { not beginning a quantifier
        const UBSALNUM     = 0o000020; // has backslash-alphanumeric in non-ARE
        const UPBOTCH      = 0o000040; // unmatched right paren in ERE
        const UBBS         = 0o000100; // backslash within bracket expr
        const UNONPOSIX    = 0o000200; // any construct extending POSIX
        const UUNSPEC      = 0o000400; // any case disallowed by POSIX
        const UUNPORT      = 0o001000; // numeric character code dependency
        const ULOCALE      = 0o002000; // locale dependency
        const UEMPTYMATCH  = 0o004000; // can match a zero-length string
        const UIMPOSSIBLE  = 0o010000; // provably cannot match anything
        const USHORTEST    = 0o020000; // has non-greedy quantifier
    }
}

bitflags! {
    /// regex compilation flags (REG_* passed to pg_regcomp). OR-able; composite
    /// members (ADVANCED, NEWLINE) are unions, like the C #defines.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RegComp: i32 {
        const BASIC    = 0o000000; // BREs (convenience; empty)
        const EXTENDED = 0o000001; // EREs
        const ADVF     = 0o000002; // advanced features in EREs
        const ADVANCED = Self::ADVF.bits() | Self::EXTENDED.bits(); // AREs
        const QUOTE    = 0o000004; // no special characters
        const ICASE    = 0o000010; // ignore case
        const NOSUB    = 0o000020; // no subexpr match data needed
        const EXPANDED = 0o000040; // whitespace & comments allowed
        const NLSTOP   = 0o000100; // \n doesn't match . or [^ ]
        const NLANCH   = 0o000200; // ^ matches after \n, $ before
        const NEWLINE  = Self::NLSTOP.bits() | Self::NLANCH.bits();
        const PEND     = 0o000400; // backward-compat hack
        const EXPECT   = 0o001000; // report partial/limited match details
        const BOSONLY  = 0o002000; // BOS-only matches kludge
        const DUMP     = 0o004000;
        const FAKE     = 0o010000;
        const PROGRESS = 0o020000;
    }
}

impl RegComp {
    /// C: `REG_NOSPEC` historical synonym for `REG_QUOTE`.
    pub const NOSPEC: Self = Self::QUOTE;
}

bitflags! {
    /// regex execution flags (REG_* passed to pg_regexec). OR-able single bits.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RegExec: i32 {
        const NOTBOL   = 0o0001; // BOS is not BOL
        const NOTEOL   = 0o0002; // EOS is not EOL
        const STARTEND = 0o0004; // backward-compat kludge
        const FTRACE   = 0o0010;
        const MTRACE   = 0o0020;
        const SMALL    = 0o0040;
    }
}

// Error / result codes. Sequential ordinals with debug/test specials and
// negative non-error results for pg_regprefix; kept as consts (not a flag set).
pub const REG_OKAY: i32 = 0;
pub const REG_NOMATCH: i32 = 1;
pub const REG_BADPAT: i32 = 2;
pub const REG_ECOLLATE: i32 = 3;
pub const REG_ECTYPE: i32 = 4;
pub const REG_EESCAPE: i32 = 5;
pub const REG_ESUBREG: i32 = 6;
pub const REG_EBRACK: i32 = 7;
pub const REG_EPAREN: i32 = 8;
pub const REG_EBRACE: i32 = 9;
pub const REG_BADBR: i32 = 10;
pub const REG_ERANGE: i32 = 11;
pub const REG_ESPACE: i32 = 12;
pub const REG_BADRPT: i32 = 13;
pub const REG_ASSERT: i32 = 15;
pub const REG_INVARG: i32 = 16;
pub const REG_MIXED: i32 = 17;
pub const REG_BADOPT: i32 = 18;
pub const REG_ETOOBIG: i32 = 19;
pub const REG_ECOLORS: i32 = 20;
pub const REG_ATOI: i32 = 101; // convert error-code name to number
pub const REG_ITOA: i32 = 102; // convert error-code number to name
pub const REG_PREFIX: i32 = -1; // pg_regprefix: identified a common prefix
pub const REG_EXACT: i32 = -2; // pg_regprefix: identified an exact match

/// C: `pg_regex_t` - a compiled RE front end. The `guts`/`fns` opaque
/// pointers stay as boxed opaque inner state. In-memory.
pub struct pg_regex_t {
    pub magic: i32,
    pub nsub: usize,        // number of subexpressions
    pub info: ReInfo,       // bitmask of features
    pub csize: i32,         // sizeof(character)
    pub endp: Option<String>, // backward-compatibility kludge
    pub collation: Oid,     // collation defining LC_CTYPE behavior
    pub guts: Option<Box<dyn core::any::Any>>, // opaque innards
    pub fns: Option<Box<dyn core::any::Any>>,
}

/// C: `pg_regmatch_t` - one reported substring.
#[derive(Debug, Clone, Copy)]
pub struct pg_regmatch_t {
    pub so: pg_regoff_t, // start of substring
    pub eo: pg_regoff_t, // end of substring
}

/// C: `rm_detail_t` - supplementary control/reporting (REG_EXPECT).
#[derive(Debug, Clone, Copy)]
pub struct rm_detail_t {
    pub rm_extend: pg_regmatch_t,
}

// regcomp.c. Returns a REG_* status code.
pub fn pg_regcomp(
    re: &mut pg_regex_t,
    string: &[pg_wchar],
    flags: RegComp,
    collation: Oid,
) -> i32 {
    unimplemented!()
}

pub fn pg_regexec(
    re: &mut pg_regex_t,
    string: &[pg_wchar],
    search_start: usize,
    details: Option<&mut rm_detail_t>,
    pmatch: &mut [pg_regmatch_t],
    flags: RegExec,
) -> i32 {
    unimplemented!()
}

/// C: `pg_regprefix(re, pg_wchar **string, size_t *slength)` - the prefix and
/// its length come back together; status (REG_PREFIX/REG_EXACT/...) is returned.
pub fn pg_regprefix(re: &mut pg_regex_t) -> (i32, Vec<pg_wchar>) {
    unimplemented!()
}

pub fn pg_regfree(re: &mut pg_regex_t) {
    unimplemented!()
}

pub fn pg_regerror(errcode: i32, preg: &pg_regex_t, errbuf: &mut [u8]) -> usize {
    unimplemented!()
}

// regexp.c
pub fn RE_compile_and_cache(text_re: &text, cflags: i32, collation: Oid) -> pg_regex_t {
    unimplemented!()
}

pub fn RE_compile_and_execute(
    text_re: &text,
    dat: &[u8],
    cflags: i32,
    collation: Oid,
    pmatch: &mut [pg_regmatch_t],
) -> bool {
    unimplemented!()
}
