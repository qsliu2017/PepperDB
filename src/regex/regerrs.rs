//! regex/regerrs.h - regex error-code -> name/explanation mapping table.
//!
//! This header has no include guard; its body is an X-macro-style list of
//! `{ REG_CODE, "REG_NAME", "explanation" }` rows that regerror.c splices into
//! its `static const struct rerr rerrs[]` via #include. We materialize that
//! projection here as a static table (rmgrlist.rs precedent). The final
//! `{-1, "", "oops"}` sentinel row lives in regerror.c, not this header, so it
//! is intentionally omitted here.

use crate::prelude::*;
use crate::regex::regerror::{
    REG_ASSERT, REG_BADBR, REG_BADOPT, REG_BADPAT, REG_BADRPT, REG_EBRACE, REG_EBRACK,
    REG_ECOLLATE, REG_ECOLORS, REG_ECTYPE, REG_EESCAPE, REG_EPAREN, REG_ERANGE, REG_ESPACE,
    REG_ESUBREG, REG_ETOOBIG, REG_INVARG, REG_MIXED, REG_NOMATCH, REG_OKAY,
};

// struct to map among codes, code names, and explanations.
// Mirrors `struct rerr` in regerror.c (a private dup also exists there).
// TODO: dedup with crate::regex::regerror once the regex subsystem is unified.
pub struct Rerr {
    pub code: c_int,
    pub name: &'static str,
    pub explain: &'static str,
}

// The error table, one row per entry of regerrs.h.
pub static REGERRS: &[Rerr] = &[
    Rerr { code: REG_OKAY, name: "REG_OKAY", explain: "no errors detected" },
    Rerr { code: REG_NOMATCH, name: "REG_NOMATCH", explain: "failed to match" },
    Rerr { code: REG_BADPAT, name: "REG_BADPAT", explain: "invalid regexp (reg version 0.8)" },
    Rerr { code: REG_ECOLLATE, name: "REG_ECOLLATE", explain: "invalid collating element" },
    Rerr { code: REG_ECTYPE, name: "REG_ECTYPE", explain: "invalid character class" },
    Rerr { code: REG_EESCAPE, name: "REG_EESCAPE", explain: "invalid escape \\ sequence" },
    Rerr { code: REG_ESUBREG, name: "REG_ESUBREG", explain: "invalid backreference number" },
    Rerr { code: REG_EBRACK, name: "REG_EBRACK", explain: "brackets [] not balanced" },
    Rerr { code: REG_EPAREN, name: "REG_EPAREN", explain: "parentheses () not balanced" },
    Rerr { code: REG_EBRACE, name: "REG_EBRACE", explain: "braces {} not balanced" },
    Rerr { code: REG_BADBR, name: "REG_BADBR", explain: "invalid repetition count(s)" },
    Rerr { code: REG_ERANGE, name: "REG_ERANGE", explain: "invalid character range" },
    Rerr { code: REG_ESPACE, name: "REG_ESPACE", explain: "out of memory" },
    Rerr { code: REG_BADRPT, name: "REG_BADRPT", explain: "quantifier operand invalid" },
    Rerr { code: REG_ASSERT, name: "REG_ASSERT", explain: "\"cannot happen\" -- you found a bug" },
    Rerr { code: REG_INVARG, name: "REG_INVARG", explain: "invalid argument to regex function" },
    Rerr { code: REG_MIXED, name: "REG_MIXED", explain: "character widths of regex and string differ" },
    Rerr { code: REG_BADOPT, name: "REG_BADOPT", explain: "invalid embedded option" },
    Rerr { code: REG_ETOOBIG, name: "REG_ETOOBIG", explain: "regular expression is too complex" },
    Rerr { code: REG_ECOLORS, name: "REG_ECOLORS", explain: "too many colors" },
];
