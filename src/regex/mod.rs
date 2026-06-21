//! Regular-expression engine (postgres/src/backend/regex + src/include/regex).
//!
//! Henry Spencer's regex library as adapted for PostgreSQL. Translated
//! incrementally; so far only the error-string mapping (regerror).

pub mod regfree;
pub mod regc_color;
pub mod regc_cvec;
pub mod regc_lex;
pub mod regc_locale;
pub mod regc_nfa;
pub mod regc_pg_locale;
pub mod regcomp;
pub mod regcustom;
pub mod rege_dfa;
pub mod regerror;
pub mod regexec;
pub mod regexport;
pub mod regprefix;
pub mod regerrs;
pub mod regex;
pub mod regguts;
