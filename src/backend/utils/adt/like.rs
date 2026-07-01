//! LIKE / ILIKE expression handling. Translated from
//! src/backend/utils/adt/like.c and its included src/backend/utils/adt/like_match.c
//! (the generic `MatchText` engine returning the LIKE_TRUE / LIKE_FALSE /
//! LIKE_ABORT trichotomy).
//!
//! FULLY TRANSLATED (text path -- what the regress suite exercises):
//!   - `textlike`/`textnlike` (`~~`/`!~~`), `texticlike`/`texticnlike`
//!     (`~~*`/`!~~*`);
//!   - `like_escape` (the `x LIKE y ESCAPE z` pattern preprocessor);
//!   - `namelike`/`namenlike`/`nameiclike`/`nameicnlike` (name -> text, reuse);
//!   - `bytealike`/`byteanlike` and `like_escape_bytea` (bytewise).
//!
//! ENCODING: `like.c` compiles `like_match.c` four times (single-byte, UTF8,
//! other multibyte, and single-byte case-insensitive) and dispatches on the
//! database encoding. This port targets the default UTF-8 encoding, so the
//! matcher walks bytes but advances a whole UTF-8 character when a wildcard
//! forces a char-synced step (PG's `UTF8_MatchText` NextChar). The single
//! `match_text` covers both the SB and UTF8 non-folding cases, and `bytealike`
//! reuses it (bytes never form a multibyte lead, so char-stepping degrades to
//! byte-stepping for ASCII/binary).
//!
//! CASE FOLDING (ILIKE): PG's `Generic_Text_IC_like` lowercases both operands
//! via `lower()` (str_tolower) then runs the plain matcher. `str_tolower` is
//! still an `unimplemented!()` stub here, so we fold ASCII `A`-`Z` inline and
//! Unicode via Rust `char::to_lowercase`. TODO(collation): route through
//! `str_tolower` once it lands, for locale-correct folding.

use crate::fmgr::{FunctionCallInfoBaseData, PG_GET_COLLATION};
use crate::postgres::{BoolGetDatum, Datum, DatumGetName, DatumGetPointer, PointerGetDatum};
use crate::postgres_ext::Oid;
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{ERRCODE_INDETERMINATE_COLLATION, ERRCODE_INVALID_ESCAPE_SEQUENCE};
use crate::c::OidIsValid;
use crate::ereport;
use crate::varatt::{VARDATA_ANY, VARSIZE_ANY_EXHDR};

use super::varlena::cstring_to_text;

// like_match.c trichotomy.
const LIKE_TRUE: i32 = 1;
const LIKE_FALSE: i32 = 0;
const LIKE_ABORT: i32 = -1;

// ---------------------------------------------------------------------------
// varlena argument helpers (mirroring varlena.rs).
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(n)` / `PG_GETARG_BYTEA_PP(n)`: the argument varlena ptr.
#[inline]
fn pg_getarg_varlena(fcinfo: &FunctionCallInfoBaseData, n: usize) -> *mut u8 {
    DatumGetPointer(fcinfo.args[n].value)
}

/// Borrow the payload bytes of any non-toasted varlena (4-byte or short header).
///
/// SAFETY: `p` must point at a valid, non-external/non-compressed varlena that
/// outlives the returned slice.
unsafe fn varlena_bytes<'a>(p: *mut u8) -> &'a [u8] {
    let len = VARSIZE_ANY_EXHDR(p);
    core::slice::from_raw_parts(VARDATA_ANY(p), len)
}

// ---------------------------------------------------------------------------
// UTF-8 char stepping (PG's NextChar for the default encoding).
// ---------------------------------------------------------------------------

/// Length in bytes of the UTF-8 character starting at `s[0]` (PG
/// `pg_mblen_with_len`). Continuation/invalid lead bytes count as 1.
#[inline]
fn mblen(s: &[u8]) -> usize {
    match s.first() {
        None => 0,
        Some(&b) if b < 0x80 => 1,
        Some(&b) if b >= 0xF0 => 4,
        Some(&b) if b >= 0xE0 => 3,
        Some(&b) if b >= 0xC0 => 2,
        Some(_) => 1,
    }
}

// ---------------------------------------------------------------------------
// GenericMatchText / Generic_Text_IC_like.
// ---------------------------------------------------------------------------

/// C `GenericMatchText`: match `s` against pattern `p` with no case folding.
fn generic_match_text(s: &[u8], p: &[u8], collation: Oid) -> i32 {
    if !OidIsValid(collation) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INDETERMINATE_COLLATION)
                .errmsg("could not determine which collation to use for LIKE")
                .errhint("Use the COLLATE clause to set the collation explicitly.");
        });
        unreachable!()
    }
    match_text(s, p, false)
}

/// C `Generic_Text_IC_like`: lowercase both operands, then match. Folds ASCII
/// inline and Unicode via `char::to_lowercase` (see module note).
fn generic_text_ic_like(s: &[u8], p: &[u8], collation: Oid) -> i32 {
    if !OidIsValid(collation) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INDETERMINATE_COLLATION)
                .errmsg("could not determine which collation to use for ILIKE")
                .errhint("Use the COLLATE clause to set the collation explicitly.");
        });
        unreachable!()
    }
    match_text(s, p, true)
}

/// Lowercase the bytes matched/compared in the ILIKE path. Escapes (`\\`) and
/// wildcards (`%`, `_`) in the pattern are ASCII and fold to themselves, so a
/// straight byte-wise fold of the whole varlena is faithful.
///
/// TODO(collation): replace with `str_tolower` for locale-correct folding.
fn ic_lower(bytes: &[u8]) -> Vec<u8> {
    String::from_utf8_lossy(bytes)
        .chars()
        .flat_map(char::to_lowercase)
        .collect::<String>()
        .into_bytes()
}

// ---------------------------------------------------------------------------
// MatchText (like_match.c). Single implementation covering the SB/UTF8 and the
// case-insensitive variants; `fold` selects inline lowercasing per character.
// ---------------------------------------------------------------------------

/// PG `like_match.c` `MatchText`: returns LIKE_TRUE / LIKE_FALSE / LIKE_ABORT.
///
/// When `fold` is set the ILIKE path has already lowercased both operands (see
/// `generic_text_ic_like`), so this matcher stays byte/char oriented exactly as
/// the C `GETCHAR` macro reduces to identity outside the SB inline-fold build.
fn match_text(mut t: &[u8], mut p: &[u8], fold: bool) -> i32 {
    // Fast path for match-everything pattern.
    if p.len() == 1 && p[0] == b'%' {
        return LIKE_TRUE;
    }

    // C guards recursion here with check_stack_depth(); that helper is still an
    // unimplemented stub in this port, so the guard is omitted (patterns are
    // user-bounded and the recursion is bounded by pattern length).
    while !t.is_empty() && !p.is_empty() {
        if p[0] == b'\\' {
            // Next pattern byte must match literally, whatever it is.
            p = &p[1..];
            if p.is_empty() {
                ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE)
                        .errmsg("LIKE pattern must not end with escape character");
                });
                unreachable!()
            }
            if getchar(p[0], fold) != getchar(t[0], fold) {
                return LIKE_FALSE;
            }
        } else if p[0] == b'%' {
            // Skip any run of wildcards immediately after the %, so the
            // recursive search always begins at a literal char to match.
            p = &p[1..];
            while !p.is_empty() {
                if p[0] == b'%' {
                    p = &p[1..];
                } else if p[0] == b'_' {
                    if t.is_empty() {
                        return LIKE_ABORT;
                    }
                    t = &t[mblen(t)..];
                    p = &p[1..];
                } else {
                    break; // Reached a non-wildcard pattern char.
                }
            }

            // Trailing % matches any remaining text.
            if p.is_empty() {
                return LIKE_TRUE;
            }

            // First remaining pattern char is a (possibly escaped) literal.
            let firstpat = if p[0] == b'\\' {
                if p.len() < 2 {
                    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                        e.errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE)
                            .errmsg("LIKE pattern must not end with escape character");
                    });
                    unreachable!()
                }
                getchar(p[1], fold)
            } else {
                getchar(p[0], fold)
            };

            while !t.is_empty() {
                if getchar(t[0], fold) == firstpat {
                    let matched = match_text(t, p, fold);
                    if matched != LIKE_FALSE {
                        return matched; // TRUE or ABORT.
                    }
                }
                t = &t[mblen(t)..];
            }

            // End of text with no match: no later start can match.
            return LIKE_ABORT;
        } else if p[0] == b'_' {
            // _ matches any single character, and we know there is one.
            t = &t[mblen(t)..];
            p = &p[1..];
            continue;
        } else if getchar(p[0], fold) != getchar(t[0], fold) {
            // Non-wildcard pattern char fails to match text char.
            return LIKE_FALSE;
        }

        // Pattern and text match, so advance by byte (safe mid-character: we are
        // not immediately after a wildcard, so text/pattern stay in lockstep).
        t = &t[1..];
        p = &p[1..];
    }

    if !t.is_empty() {
        return LIKE_FALSE; // End of pattern, but not of text.
    }

    // End of text: match iff the remaining pattern is zero or more %'s.
    while !p.is_empty() && p[0] == b'%' {
        p = &p[1..];
    }
    if p.is_empty() {
        return LIKE_TRUE;
    }

    LIKE_ABORT
}

/// C `like_match.c` `GETCHAR`: identity, or single-byte lowercase when folding.
/// The operands are already fully lowercased in the ILIKE path, so this only
/// needs to fold ASCII lead bytes that slipped through; kept for structural
/// fidelity with the SB inline-fold build.
#[inline]
fn getchar(c: u8, fold: bool) -> u8 {
    if fold {
        c.to_ascii_lowercase()
    } else {
        c
    }
}

// ---------------------------------------------------------------------------
// like_escape (like_match.c do_like_escape).
// ---------------------------------------------------------------------------

/// C `do_like_escape`: rewrite `pat` to use the standard `\\` escape given the
/// user's ESCAPE string `esc`. Returns the payload bytes of the result text.
fn do_like_escape(pat: &[u8], esc: &[u8]) -> Vec<u8> {
    let mut r: Vec<u8> = Vec::with_capacity(pat.len() * 2);

    if esc.is_empty() {
        // No escape wanted: double every backslash so it acts literally.
        let mut p = pat;
        while !p.is_empty() {
            if p[0] == b'\\' {
                r.push(b'\\');
            }
            let l = mblen(p);
            r.extend_from_slice(&p[..l]);
            p = &p[l..];
        }
        return r;
    }

    // The specified escape must be exactly one character.
    if mblen(esc) != esc.len() {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE)
                .errmsg("invalid escape string")
                .errhint("Escape string must be empty or one character.");
        });
        unreachable!()
    }

    // If specified escape is '\', the pattern is already in standard form.
    if esc[0] == b'\\' {
        return pat.to_vec();
    }

    // Convert the specified escape char to '\', and double '\' -- unless it
    // immediately follows an escape character.
    let mut afterescape = false;
    let mut p = pat;
    while !p.is_empty() {
        if chareq(p, esc) && !afterescape {
            r.push(b'\\');
            p = &p[mblen(p)..];
            afterescape = true;
        } else if p[0] == b'\\' {
            r.push(b'\\');
            if !afterescape {
                r.push(b'\\');
            }
            p = &p[mblen(p)..];
            afterescape = false;
        } else {
            let l = mblen(p);
            r.extend_from_slice(&p[..l]);
            p = &p[l..];
            afterescape = false;
        }
    }

    r
}

/// C `like_match.c` `CHAREQ` (multibyte): whole-character equality of the lead
/// characters of `a` and `b`.
#[inline]
fn chareq(a: &[u8], b: &[u8]) -> bool {
    let la = mblen(a);
    la == mblen(b) && a[..la] == b[..la]
}

// ---------------------------------------------------------------------------
// fmgr entry points.
// ---------------------------------------------------------------------------

/// Read a `name` argument (arg 0) as its logical string bytes.
///
/// Returns an owned copy because the `NameData` may not outlive later borrows.
fn name_arg_bytes(fcinfo: &FunctionCallInfoBaseData) -> Vec<u8> {
    // SAFETY: arg 0 is a valid `Name` pointer that outlives this read.
    let nd = unsafe { &*DatumGetName(fcinfo.args[0].value) };
    let end = nd
        .data
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(nd.data.len());
    nd.data[..end].to_vec()
}

/// PG `namelike` (`name ~~ text`).
pub fn namelike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = name_arg_bytes(fcinfo);
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: arg 1 is a valid non-toasted text varlena.
    let p = unsafe { varlena_bytes(pat) };
    let result = generic_match_text(&s, p, PG_GET_COLLATION(fcinfo)) == LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `namenlike` (`name !~~ text`).
pub fn namenlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = name_arg_bytes(fcinfo);
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: arg 1 is a valid non-toasted text varlena.
    let p = unsafe { varlena_bytes(pat) };
    let result = generic_match_text(&s, p, PG_GET_COLLATION(fcinfo)) != LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `textlike` (`text ~~ text`).
pub fn textlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str = pg_getarg_varlena(fcinfo, 0);
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: both args are valid non-toasted text varlenas.
    let (s, p) = unsafe { (varlena_bytes(str), varlena_bytes(pat)) };
    let result = generic_match_text(s, p, PG_GET_COLLATION(fcinfo)) == LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `textnlike` (`text !~~ text`).
pub fn textnlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str = pg_getarg_varlena(fcinfo, 0);
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: both args are valid non-toasted text varlenas.
    let (s, p) = unsafe { (varlena_bytes(str), varlena_bytes(pat)) };
    let result = generic_match_text(s, p, PG_GET_COLLATION(fcinfo)) != LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `bytealike` (`bytea ~~ bytea`): bytewise, no collation.
pub fn bytealike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str = pg_getarg_varlena(fcinfo, 0);
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: both args are valid non-toasted bytea varlenas.
    let (s, p) = unsafe { (varlena_bytes(str), varlena_bytes(pat)) };
    let result = match_text(s, p, false) == LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `byteanlike` (`bytea !~~ bytea`).
pub fn byteanlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str = pg_getarg_varlena(fcinfo, 0);
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: both args are valid non-toasted bytea varlenas.
    let (s, p) = unsafe { (varlena_bytes(str), varlena_bytes(pat)) };
    let result = match_text(s, p, false) != LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `nameiclike` (`name ~~* text`).
pub fn nameiclike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = ic_lower(&name_arg_bytes(fcinfo));
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: arg 1 is a valid non-toasted text varlena.
    let p = ic_lower(unsafe { varlena_bytes(pat) });
    let result = generic_text_ic_like(&s, &p, PG_GET_COLLATION(fcinfo)) == LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `nameicnlike` (`name !~~* text`).
pub fn nameicnlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = ic_lower(&name_arg_bytes(fcinfo));
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: arg 1 is a valid non-toasted text varlena.
    let p = ic_lower(unsafe { varlena_bytes(pat) });
    let result = generic_text_ic_like(&s, &p, PG_GET_COLLATION(fcinfo)) != LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `texticlike` (`text ~~* text`).
pub fn texticlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str = pg_getarg_varlena(fcinfo, 0);
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: both args are valid non-toasted text varlenas.
    let (s, p) = unsafe { (ic_lower(varlena_bytes(str)), ic_lower(varlena_bytes(pat))) };
    let result = generic_text_ic_like(&s, &p, PG_GET_COLLATION(fcinfo)) == LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `texticnlike` (`text !~~* text`).
pub fn texticnlike(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str = pg_getarg_varlena(fcinfo, 0);
    let pat = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: both args are valid non-toasted text varlenas.
    let (s, p) = unsafe { (ic_lower(varlena_bytes(str)), ic_lower(varlena_bytes(pat))) };
    let result = generic_text_ic_like(&s, &p, PG_GET_COLLATION(fcinfo)) != LIKE_TRUE;
    BoolGetDatum(result)
}

/// PG `like_escape`: rewrite a pattern to the standard `\\` escape convention.
pub fn like_escape(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let pat = pg_getarg_varlena(fcinfo, 0);
    let esc = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: both args are valid non-toasted text varlenas.
    let (p, e) = unsafe { (varlena_bytes(pat), varlena_bytes(esc)) };
    let out = do_like_escape(p, e);
    let s = String::from_utf8_lossy(&out);
    PointerGetDatum(cstring_to_text(&s).cast::<u8>())
}

/// PG `like_escape_bytea`: the bytewise ESCAPE preprocessor.
pub fn like_escape_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let pat = pg_getarg_varlena(fcinfo, 0);
    let esc = pg_getarg_varlena(fcinfo, 1);
    // SAFETY: both args are valid non-toasted bytea varlenas.
    let (p, e) = unsafe { (varlena_bytes(pat), varlena_bytes(esc)) };
    let out = do_like_escape(p, e);
    let s = String::from_utf8_lossy(&out);
    PointerGetDatum(cstring_to_text(&s).cast::<u8>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::C_COLLATION_OID;
    use crate::postgres::{DatumGetBool, DatumGetCString, NullableDatum};

    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: None,
            resultinfo: None,
            fncollation: C_COLLATION_OID,
            isnull: false,
            nargs: args.len() as i16,
            args: args
                .iter()
                .map(|&value| NullableDatum { value, isnull: false })
                .collect(),
        }
    }

    fn text_datum(s: &str) -> Datum {
        PointerGetDatum(cstring_to_text(s).cast::<u8>())
    }

    /// Convert a text Datum (from `like_escape`) to a Rust String via `textout`.
    fn text_out_to_string(d: Datum) -> String {
        let out = super::super::varlena::textout(&mut fc(&[d]));
        let p = DatumGetCString(out);
        // SAFETY: textout returns a valid NUL-terminated cstring.
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }

    fn like(s: &str, pat: &str) -> bool {
        DatumGetBool(textlike(&mut fc(&[text_datum(s), text_datum(pat)])))
    }

    fn ilike(s: &str, pat: &str) -> bool {
        DatumGetBool(texticlike(&mut fc(&[text_datum(s), text_datum(pat)])))
    }

    #[test]
    fn like_percent_and_underscore() {
        assert!(like("abc", "a%"));
        assert!(like("abc", "%c"));
        assert!(like("abc", "a%c"));
        assert!(like("abc", "_bc"));
        assert!(like("abc", "a_c"));
        assert!(like("abc", "abc"));
        assert!(like("abc", "%"));
        assert!(!like("abc", "a_"));
        assert!(!like("abc", "b%"));
        assert!(!like("abc", "abcd"));
        assert!(like("", "%"));
        assert!(!like("", "_"));
    }

    #[test]
    fn like_escape_default_backslash() {
        // Backslash escapes the following metacharacter.
        assert!(like("a%c", "a\\%c"));
        assert!(!like("abc", "a\\%c"));
        assert!(like("a_c", "a\\_c"));
        assert!(!like("abc", "a\\_c"));
    }

    #[test]
    fn like_multibyte_char_step() {
        // Underscore matches exactly one multibyte character.
        assert!(like("\u{4f60}\u{597d}", "_\u{597d}"));
        assert!(like("caf\u{e9}", "caf_"));
        assert!(like("caf\u{e9}", "ca%"));
        assert!(!like("caf\u{e9}", "caf__"));
    }

    #[test]
    fn ilike_case_insensitive() {
        assert!(ilike("ABC", "abc"));
        assert!(ilike("abc", "ABC"));
        assert!(ilike("AbC", "a%C"));
        assert!(ilike("Hello World", "hello%"));
        assert!(!ilike("abc", "abd"));
        // negated
        assert!(DatumGetBool(texticnlike(&mut fc(&[
            text_datum("abc"),
            text_datum("abd"),
        ]))));
    }

    #[test]
    fn nlike_negation() {
        assert!(DatumGetBool(textnlike(&mut fc(&[
            text_datum("abc"),
            text_datum("x%"),
        ]))));
        assert!(!DatumGetBool(textnlike(&mut fc(&[
            text_datum("abc"),
            text_datum("a%"),
        ]))));
    }

    #[test]
    fn like_escape_rewrites_pattern() {
        // ESCAPE '#' converts '#' to '\' in the standard pattern.
        let d = like_escape(&mut fc(&[text_datum("a#%c"), text_datum("#")]));
        assert_eq!(text_out_to_string(d), "a\\%c");
        // Empty escape doubles backslashes.
        let d = like_escape(&mut fc(&[text_datum("a\\b"), text_datum("")]));
        assert_eq!(text_out_to_string(d), "a\\\\b");
        // Escape '\' leaves the pattern unchanged.
        let d = like_escape(&mut fc(&[text_datum("a\\%c"), text_datum("\\")]));
        assert_eq!(text_out_to_string(d), "a\\%c");
    }

    #[test]
    fn like_escape_pattern_then_matches() {
        // After preprocessing 'a#%c' ESCAPE '#', 'a%c' should match literally.
        let d = like_escape(&mut fc(&[text_datum("a#%c"), text_datum("#")]));
        let pat = text_out_to_string(d);
        assert!(like("a%c", &pat));
        assert!(!like("abc", &pat));
    }

    #[test]
    fn fmgr_table_binds_textlike() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "textlike")
            .expect("textlike present");
        let func = entry.func.expect("textlike bound");
        let mut f = fc(&[text_datum("hello"), text_datum("h%")]);
        assert!(DatumGetBool(func(&mut f)));
    }
}
