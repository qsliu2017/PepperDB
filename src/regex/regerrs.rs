//! Translated from PostgreSQL src/include/regex/regerrs.h
//
// This header is a `{code, "NAME", "description"}` initializer fragment included
// into a table in regex.c. Translated to a static table. The REG_* codes are
// defined in regex/regex.h (not in this batch).

/// One entry of the regex error table: (code, symbol name, human description).
pub struct RegError {
    pub code: i32,
    pub name: &'static str,
    pub explain: &'static str,
}

// REG_* numeric codes mirror regex/regex.h ordering (REG_OKAY = 0, ...).
pub const REG_ERRORS: &[RegError] = &[
    RegError { code: 0, name: "REG_OKAY", explain: "no errors detected" },
    RegError { code: 1, name: "REG_NOMATCH", explain: "failed to match" },
    RegError { code: 2, name: "REG_BADPAT", explain: "invalid regexp (reg version 0.8)" },
    RegError { code: 3, name: "REG_ECOLLATE", explain: "invalid collating element" },
    RegError { code: 4, name: "REG_ECTYPE", explain: "invalid character class" },
    RegError { code: 5, name: "REG_EESCAPE", explain: "invalid escape \\ sequence" },
    RegError { code: 6, name: "REG_ESUBREG", explain: "invalid backreference number" },
    RegError { code: 7, name: "REG_EBRACK", explain: "brackets [] not balanced" },
    RegError { code: 8, name: "REG_EPAREN", explain: "parentheses () not balanced" },
    RegError { code: 9, name: "REG_EBRACE", explain: "braces {} not balanced" },
    RegError { code: 10, name: "REG_BADBR", explain: "invalid repetition count(s)" },
    RegError { code: 11, name: "REG_ERANGE", explain: "invalid character range" },
    RegError { code: 12, name: "REG_ESPACE", explain: "out of memory" },
    RegError { code: 13, name: "REG_BADRPT", explain: "quantifier operand invalid" },
    RegError { code: 14, name: "REG_ASSERT", explain: "\"cannot happen\" -- you found a bug" },
    RegError { code: 15, name: "REG_INVARG", explain: "invalid argument to regex function" },
    RegError { code: 16, name: "REG_MIXED", explain: "character widths of regex and string differ" },
    RegError { code: 17, name: "REG_BADOPT", explain: "invalid embedded option" },
    RegError { code: 18, name: "REG_ETOOBIG", explain: "regular expression is too complex" },
    RegError { code: 19, name: "REG_ECOLORS", explain: "too many colors" },
];
