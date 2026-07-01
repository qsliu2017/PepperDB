//! SQL regexp functions. Translated in spirit from
//! src/backend/utils/adt/regexp.c over the hand-written POSIX ARE engine in
//! `crate::regex::regex` (NOT the Rust `regex` crate: PG's `~`/`SIMILAR TO`
//! semantics differ, so the engine's behavior is preserved).
//!
//! FULLY TRANSLATED (the leverage for the `name`/`strings` conformance tests):
//!   - the operator functions `textregexeq`/`textregexne` (`~`/`!~`),
//!     `texticregexeq`/`texticregexne` (`~*`/`!~*`), and their `name`-arg
//!     variants (`nameregexeq`/`nameregexne`/`nameicregexeq`/`nameicregexne`);
//!   - `regexp_match` (first match, `_text` array of captures) and its
//!     no-flags variant;
//!   - `regexp_replace` in all its arities, including the `\1..\9` and `\&`
//!     backreference substitution and the `g` (global) flag;
//!   - `regexp_like`, `regexp_count`, `regexp_instr`, `regexp_substr` scalars.
//!
//! STAGED (need the set-returning-function protocol / array-type machinery that
//! is reachable only once step-08 SRF + the array subsystem land; each is a
//! precise `unimplemented!` with the blocking reason):
//!   - `regexp_matches` / `regexp_matches_no_flags` (SRF over all matches);
//!   - `regexp_split_to_table` (SRF), `regexp_split_to_array` (array result).
//!
//! ENCODING: the server encoding is UTF-8, so patterns/subjects are handled as
//! `&str` and the engine returns byte offsets into the original text.

use crate::c::text;
use crate::ereport;
use crate::fmgr::{FunctionCallInfoBaseData, PG_GET_COLLATION, PG_NARGS};
use crate::postgres::{
    BoolGetDatum, Datum, DatumGetInt32, DatumGetName, DatumGetPointer, Int32GetDatum,
    PointerGetDatum,
};
use crate::regex::regex::{Regex, RegComp};
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_REGULAR_EXPRESSION,
};
use crate::varatt::{VARDATA_ANY, VARSIZE_ANY_EXHDR};

use super::varlena::cstring_to_text;

// ---------------------------------------------------------------------------
// varlena / name argument helpers (mirroring varlena.rs + like.rs).
// ---------------------------------------------------------------------------

#[inline]
fn pg_getarg_varlena(fcinfo: &FunctionCallInfoBaseData, n: usize) -> *mut u8 {
    DatumGetPointer(fcinfo.args[n].value)
}

/// SAFETY: `p` must point at a valid non-toasted varlena that outlives the slice.
unsafe fn varlena_bytes<'a>(p: *mut u8) -> &'a [u8] {
    let len = VARSIZE_ANY_EXHDR(p);
    core::slice::from_raw_parts(VARDATA_ANY(p), len)
}

/// Read a text arg as an owned UTF-8 String (lossy for invalid bytes).
fn text_arg_string(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = pg_getarg_varlena(fcinfo, n);
    // SAFETY: arg `n` is a valid non-toasted text varlena.
    let bytes = unsafe { varlena_bytes(p) };
    String::from_utf8_lossy(bytes).into_owned()
}

/// Read a `name` arg (arg 0) as an owned UTF-8 String.
fn name_arg_string(fcinfo: &FunctionCallInfoBaseData) -> String {
    // SAFETY: arg 0 is a valid `Name` pointer that outlives this read.
    let nd = unsafe { &*DatumGetName(fcinfo.args[0].value) };
    let end = nd.data.iter().position(|&b| b == 0).unwrap_or(nd.data.len());
    String::from_utf8_lossy(&nd.data[..end]).into_owned()
}

// ---------------------------------------------------------------------------
// Flag parsing (parse_re_flags) + compile helpers.
// ---------------------------------------------------------------------------

/// The parsed regexp option surface: the RegComp flags plus the global flag.
struct ReFlags {
    cflags: RegComp,
    glob: bool,
}

/// PG `parse_re_flags`: default is REG_ADVANCED; letters tweak the flavor.
/// Raises `invalid regular expression option: "z"` for an unknown letter.
fn parse_re_flags(opts: Option<&str>) -> ReFlags {
    let mut cflags = RegComp::ADVANCED;
    let mut glob = false;
    if let Some(opts) = opts {
        for ch in opts.chars() {
            match ch {
                'g' => glob = true,
                'b' => cflags &= !(RegComp::ADVANCED | RegComp::EXTENDED | RegComp::QUOTE),
                'c' => cflags &= !RegComp::ICASE,
                'e' => {
                    cflags |= RegComp::EXTENDED;
                    cflags &= !(RegComp::ADVANCED | RegComp::QUOTE);
                }
                'i' => cflags |= RegComp::ICASE,
                'm' | 'n' => cflags |= RegComp::NEWLINE,
                'p' => {
                    cflags |= RegComp::NLSTOP;
                    cflags &= !RegComp::NLANCH;
                }
                'q' => {
                    cflags |= RegComp::QUOTE;
                    cflags &= !(RegComp::ADVANCED | RegComp::EXTENDED);
                }
                's' => cflags &= !RegComp::NEWLINE,
                't' => cflags &= !RegComp::EXPANDED,
                'w' => {
                    cflags &= !RegComp::NLSTOP;
                    cflags |= RegComp::NLANCH;
                }
                'x' => cflags |= RegComp::EXPANDED,
                other => ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("invalid regular expression option: \"{other}\""));
                }),
            }
        }
    }
    ReFlags { cflags, glob }
}

/// Compile `pattern` with `cflags`, raising the PG regex error on failure.
/// QUOTE mode is handled by escaping the whole pattern to a literal.
fn compile(pattern: &str, cflags: RegComp) -> Regex {
    if cflags.contains(RegComp::QUOTE) {
        let escaped = quote_literal_pattern(pattern);
        return compile(&escaped, cflags & !RegComp::QUOTE);
    }
    match Regex::compile(pattern, cflags) {
        Ok(re) => re,
        Err(err) => {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_REGULAR_EXPRESSION)
                    .errmsg(err.message.clone());
            });
            unreachable!()
        }
    }
}

/// Escape every regex metacharacter so the pattern matches literally (REG_QUOTE).
fn quote_literal_pattern(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() * 2);
    for c in pattern.chars() {
        if "\\.^$|()[]{}*+?".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

// ---------------------------------------------------------------------------
// Operator functions: ~ !~ ~* !~*
// ---------------------------------------------------------------------------

fn regex_matches(subject: &str, pattern: &str, cflags: RegComp) -> bool {
    compile(pattern, cflags).exec(subject, 0).is_some()
}

macro_rules! regex_op {
    ($name:ident, $icase:expr, $negate:expr, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let subject = text_arg_string(fcinfo, 0);
            let pattern = text_arg_string(fcinfo, 1);
            let mut cflags = RegComp::ADVANCED;
            if $icase {
                cflags |= RegComp::ICASE;
            }
            let _ = PG_GET_COLLATION(fcinfo); // collation drives LC_CTYPE (default here)
            let m = regex_matches(&subject, &pattern, cflags);
            BoolGetDatum(m ^ $negate)
        }
    };
}

regex_op!(textregexeq, false, false, "PG `textregexeq` (`text ~ text`).");
regex_op!(textregexne, false, true, "PG `textregexne` (`text !~ text`).");
regex_op!(texticregexeq, true, false, "PG `texticregexeq` (`text ~* text`).");
regex_op!(texticregexne, true, true, "PG `texticregexne` (`text !~* text`).");

macro_rules! regex_name_op {
    ($name:ident, $icase:expr, $negate:expr, $doc:literal) => {
        #[doc = $doc]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let subject = name_arg_string(fcinfo);
            let pattern = text_arg_string(fcinfo, 1);
            let mut cflags = RegComp::ADVANCED;
            if $icase {
                cflags |= RegComp::ICASE;
            }
            let _ = PG_GET_COLLATION(fcinfo);
            let m = regex_matches(&subject, &pattern, cflags);
            BoolGetDatum(m ^ $negate)
        }
    };
}

regex_name_op!(nameregexeq, false, false, "PG `nameregexeq` (`name ~ text`).");
regex_name_op!(nameregexne, false, true, "PG `nameregexne` (`name !~ text`).");
regex_name_op!(nameicregexeq, true, false, "PG `nameicregexeq` (`name ~* text`).");
regex_name_op!(nameicregexne, true, true, "PG `nameicregexne` (`name !~* text`).");

// ---------------------------------------------------------------------------
// regexp_like (bool test)
// ---------------------------------------------------------------------------

/// PG `regexp_like` / `regexp_like_no_flags`: bool test for a match.
pub fn regexp_like(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let subject = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let flags = if PG_NARGS(fcinfo) > 2 {
        Some(text_arg_string(fcinfo, 2))
    } else {
        None
    };
    let re = parse_re_flags(flags.as_deref());
    if re.glob {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("regexp_like() does not support the \"global\" option");
        });
    }
    BoolGetDatum(compile(&pattern, re.cflags).exec(&subject, 0).is_some())
}

/// PG `regexp_like_no_flags`: same as `regexp_like` with no flags string.
pub fn regexp_like_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    regexp_like(fcinfo)
}

// ---------------------------------------------------------------------------
// regexp_replace
// ---------------------------------------------------------------------------

/// PG `textregexreplace_noopt`: `regexp_replace(source, pattern, replacement)`.
pub fn textregexreplace_noopt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let source = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let replacement = text_arg_string(fcinfo, 2);
    let _ = PG_GET_COLLATION(fcinfo);
    let out = do_replace(&source, &pattern, &replacement, RegComp::ADVANCED, false, 1, 0);
    PointerGetDatum(cstring_to_text(&out).cast::<u8>())
}

/// PG `textregexreplace`: `regexp_replace(source, pattern, replacement, flags)`.
pub fn textregexreplace(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let source = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let replacement = text_arg_string(fcinfo, 2);
    let flags = text_arg_string(fcinfo, 3);
    let _ = PG_GET_COLLATION(fcinfo);
    let re = parse_re_flags(Some(&flags));
    let out = do_replace(&source, &pattern, &replacement, re.cflags, re.glob, 1, 0);
    PointerGetDatum(cstring_to_text(&out).cast::<u8>())
}

/// PG `textregexreplace_extended`:
/// `regexp_replace(source, pattern, replacement, start, N, flags)`.
pub fn textregexreplace_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    regexreplace_extended_impl(fcinfo, true, true)
}

/// PG `textregexreplace_extended_no_flags`: like extended without the flags arg.
pub fn textregexreplace_extended_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    regexreplace_extended_impl(fcinfo, true, false)
}

/// PG `textregexreplace_extended_no_n`: like extended without N or flags args.
pub fn textregexreplace_extended_no_n(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    regexreplace_extended_impl(fcinfo, false, false)
}

fn regexreplace_extended_impl(
    fcinfo: &FunctionCallInfoBaseData,
    have_n: bool,
    have_flags: bool,
) -> Datum {
    let source = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let replacement = text_arg_string(fcinfo, 2);
    let start = DatumGetInt32(fcinfo.args[3].value);
    if start < 1 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("invalid value for parameter \"start\": must be positive");
        });
    }
    let n = if have_n { DatumGetInt32(fcinfo.args[4].value).max(0) } else { 0 };
    let flags = if have_flags {
        Some(text_arg_string(fcinfo, 5))
    } else {
        None
    };
    let re = parse_re_flags(flags.as_deref());
    let _ = PG_GET_COLLATION(fcinfo);
    // With N specified (and not glob), replace only the Nth match; N==0 means
    // "all from `start`". Without an N arg, PG defaults to global replacement.
    let global = re.glob || !have_n;
    let out = do_replace(
        &source,
        &pattern,
        &replacement,
        re.cflags,
        global,
        start as usize,
        n as usize,
    );
    PointerGetDatum(cstring_to_text(&out).cast::<u8>())
}

/// Core replacement over UTF-8 byte offsets.
///
/// `start` is a 1-based *character* position to begin scanning; `nth` (when
/// non-zero) selects a single match to replace, else `global` decides whether
/// all-from-start or just the first match is replaced.
#[allow(
    clippy::too_many_arguments,
    reason = "faithful to regexp_replace's parameter set (source/pattern/repl/\
              flags/global/start/nth); a struct would obscure the 1:1 mapping"
)]
fn do_replace(
    source: &str,
    pattern: &str,
    replacement: &str,
    cflags: RegComp,
    global: bool,
    start: usize,
    nth: usize,
) -> String {
    let re = compile(pattern, cflags);
    // Byte offset of the 1-based char `start`.
    let start_byte = char_to_byte(source, start.saturating_sub(1));
    let mut out = String::with_capacity(source.len());
    out.push_str(&source[..start_byte]);

    let mut search = start_byte;
    let mut match_no = 0usize;
    while let Some(caps) = re.exec(source, search) {
        let Some((mstart, mend)) = caps.first().copied().flatten() else {
            break;
        };
        match_no += 1;
        // Emit unmatched text preceding this match.
        out.push_str(&source[search..mstart]);

        let replace_here = if nth > 0 {
            match_no == nth
        } else {
            global || match_no == 1
        };
        if replace_here {
            expand_replacement(&mut out, replacement, source, &caps);
        } else {
            out.push_str(&source[mstart..mend]);
        }

        // Advance; handle zero-width matches by stepping one char.
        if mend > search {
            search = mend;
        } else if let Some(ch) = source[mend..].chars().next() {
            out.push(ch);
            search = mend + ch.len_utf8();
        } else {
            search = mend + 1;
        }
        // Stop early when only a single (first or Nth) replacement is wanted.
        if !global && nth == 0 {
            break;
        }
        if nth > 0 && match_no >= nth {
            break;
        }
        if search > source.len() {
            break;
        }
    }
    out.push_str(&source[search.min(source.len())..]);
    out
}

/// Expand a `replacement` string, substituting `\1..\9` capture groups and
/// `\&` (whole match); `\\` yields a literal backslash.
fn expand_replacement(
    out: &mut String,
    replacement: &str,
    source: &str,
    caps: &[Option<(usize, usize)>],
) {
    let bytes: Vec<char> = replacement.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == '\\' && i + 1 < bytes.len() {
            let n = bytes[i + 1];
            if let Some(d) = n.to_digit(10) {
                if let Some(Some((s, e))) = caps.get(d as usize) {
                    out.push_str(&source[*s..*e]);
                }
                i += 2;
                continue;
            }
            if n == '&' {
                if let Some(Some((s, e))) = caps.first() {
                    out.push_str(&source[*s..*e]);
                }
                i += 2;
                continue;
            }
            // `\\` -> `\`; any other `\x` -> literal `x`.
            out.push(n);
            i += 2;
            continue;
        }
        if c == '&' {
            if let Some(Some((s, e))) = caps.first() {
                out.push_str(&source[*s..*e]);
            }
            i += 1;
            continue;
        }
        out.push(c);
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// regexp_match (first match; array of captures)
// ---------------------------------------------------------------------------

/// PG `regexp_match` / `regexp_match_no_flags`. Returns a `_text` array of the
/// capture groups of the first match (group 0 when there are no groups), or
/// NULL when there is no match.
///
/// The array result needs the array-type constructor; that path is staged.
pub fn regexp_match(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let subject = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let flags = if PG_NARGS(fcinfo) > 2 {
        Some(text_arg_string(fcinfo, 2))
    } else {
        None
    };
    let re_flags = parse_re_flags(flags.as_deref());
    if re_flags.glob {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("regexp_match() does not support the \"global\" option");
        });
    }
    let re = compile(&pattern, re_flags.cflags);
    let Some(caps) = re.exec(&subject, 0) else {
        fcinfo.isnull = true;
        return Datum(0);
    };
    let _ = extract_match_groups(&subject, &caps, re.ngroups());
    unimplemented!(
        "regexp_match array result needs construct_md_array/text[] builder \
         (array subsystem, deferred)"
    );
}

/// PG `regexp_match_no_flags`.
pub fn regexp_match_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    regexp_match(fcinfo)
}

/// Collect the capture-group substrings of one match into owned strings
/// (group 0 alone when the pattern has no capturing groups), NULL for
/// non-participating groups.
fn extract_match_groups(
    source: &str,
    caps: &[Option<(usize, usize)>],
    ngroups: usize,
) -> Vec<Option<String>> {
    if ngroups == 0 {
        return vec![caps.first().and_then(|o| o.map(|(s, e)| source[s..e].to_string()))];
    }
    caps.iter()
        .skip(1)
        .take(ngroups)
        .map(|o| o.map(|(s, e)| source[s..e].to_string()))
        .collect()
}

// ---------------------------------------------------------------------------
// regexp_matches / regexp_split_to_table / regexp_split_to_array (STAGED)
// ---------------------------------------------------------------------------

/// PG `regexp_matches`: SRF over every match. STAGED on the SRF protocol.
pub fn regexp_matches(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!(
        "regexp_matches is a set-returning function needing the SRF \
         ValuePerCall/materialize protocol + text[] array builder (deferred)"
    );
}

/// PG `regexp_matches_no_flags`. STAGED with `regexp_matches`.
pub fn regexp_matches_no_flags(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!(
        "regexp_matches_no_flags is a set-returning function (see regexp_matches)"
    );
}

/// PG `regexp_split_to_table`: SRF of the split fields. STAGED on SRF protocol.
pub fn regexp_split_to_table(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!(
        "regexp_split_to_table is a set-returning function needing the SRF protocol"
    );
}

/// PG `regexp_split_to_table_no_flags`. STAGED with `regexp_split_to_table`.
pub fn regexp_split_to_table_no_flags(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!(
        "regexp_split_to_table_no_flags is a set-returning function (see above)"
    );
}

/// PG `regexp_split_to_array`: array of split fields. STAGED on array builder.
pub fn regexp_split_to_array(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("regexp_split_to_array needs the text[] array builder (deferred)");
}

/// PG `regexp_split_to_array_no_flags`. STAGED with `regexp_split_to_array`.
pub fn regexp_split_to_array_no_flags(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("regexp_split_to_array_no_flags needs the text[] array builder");
}

// ---------------------------------------------------------------------------
// regexp_count / regexp_instr / regexp_substr (scalars)
// ---------------------------------------------------------------------------

/// Iterate matches of `re` in `source` from byte offset `start_byte`, invoking
/// `f(match_no, mstart, mend, caps)` for each. Zero-width matches advance a char.
fn for_each_match(
    re: &Regex,
    source: &str,
    start_byte: usize,
    mut f: impl FnMut(usize, usize, usize, &[Option<(usize, usize)>]),
) {
    let mut search = start_byte;
    let mut n = 0usize;
    while search <= source.len() {
        let Some(caps) = re.exec(source, search) else { break };
        let Some((s, e)) = caps.first().copied().flatten() else { break };
        n += 1;
        f(n, s, e, &caps);
        if e > search {
            search = e;
        } else {
            search = e + source[e..].chars().next().map_or(1, char::len_utf8);
        }
    }
}

fn char_to_byte(s: &str, char_idx: usize) -> usize {
    s.char_indices().nth(char_idx).map_or(s.len(), |(b, _)| b)
}

fn byte_to_char(s: &str, byte_idx: usize) -> usize {
    s[..byte_idx.min(s.len())].chars().count()
}

/// PG `regexp_count`: count matches from an optional start, with flags.
pub fn regexp_count(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let subject = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let nargs = PG_NARGS(fcinfo);
    let start = if nargs > 2 { DatumGetInt32(fcinfo.args[2].value) } else { 1 };
    if start < 1 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("invalid value for parameter \"start\": must be positive");
        });
    }
    let flags = if nargs > 3 { Some(text_arg_string(fcinfo, 3)) } else { None };
    let re = compile(&pattern, parse_re_flags(flags.as_deref()).cflags);
    let start_byte = char_to_byte(&subject, (start - 1) as usize);
    let mut count = 0i32;
    for_each_match(&re, &subject, start_byte, |_, _, _, _| count += 1);
    Int32GetDatum(count)
}

/// PG `regexp_count_no_start` (2-arg).
pub fn regexp_count_no_start(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    regexp_count(fcinfo)
}

/// PG `regexp_count_no_flags` (3-arg).
pub fn regexp_count_no_flags(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    regexp_count(fcinfo)
}

/// PG `regexp_instr`: 1-based char position of the Nth match (0 if none).
/// Only the common 2/3/4-arg forms (no subexpr/endoption tuning) are honored;
/// endoption/subexpr default to 0.
pub fn regexp_instr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let subject = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let nargs = PG_NARGS(fcinfo);
    let start = if nargs > 2 { DatumGetInt32(fcinfo.args[2].value) } else { 1 };
    let want_n = if nargs > 3 { DatumGetInt32(fcinfo.args[3].value).max(1) } else { 1 };
    let endoption = if nargs > 4 { DatumGetInt32(fcinfo.args[4].value) } else { 0 };
    let flags = if nargs > 5 { Some(text_arg_string(fcinfo, 5)) } else { None };
    if start < 1 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("invalid value for parameter \"start\": must be positive");
        });
    }
    let re = compile(&pattern, parse_re_flags(flags.as_deref()).cflags);
    let start_byte = char_to_byte(&subject, (start - 1) as usize);
    let mut result = 0i32;
    for_each_match(&re, &subject, start_byte, |n, s, e, _| {
        if n as i32 == want_n {
            let pos = if endoption == 1 { e } else { s };
            result = (byte_to_char(&subject, pos) + 1) as i32;
        }
    });
    Int32GetDatum(result)
}

macro_rules! regexp_instr_alias {
    ($name:ident) => {
        #[doc = concat!("PG `", stringify!($name), "` (arity variant of regexp_instr).")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            regexp_instr(fcinfo)
        }
    };
}
regexp_instr_alias!(regexp_instr_no_start);
regexp_instr_alias!(regexp_instr_no_n);
regexp_instr_alias!(regexp_instr_no_endoption);
regexp_instr_alias!(regexp_instr_no_flags);
regexp_instr_alias!(regexp_instr_no_subexpr);

/// PG `regexp_substr`: the substring of the Nth match, or NULL if none.
pub fn regexp_substr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let subject = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let nargs = PG_NARGS(fcinfo);
    let start = if nargs > 2 { DatumGetInt32(fcinfo.args[2].value) } else { 1 };
    let want_n = if nargs > 3 { DatumGetInt32(fcinfo.args[3].value).max(1) } else { 1 };
    let flags = if nargs > 4 { Some(text_arg_string(fcinfo, 4)) } else { None };
    if start < 1 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("invalid value for parameter \"start\": must be positive");
        });
    }
    let re = compile(&pattern, parse_re_flags(flags.as_deref()).cflags);
    let start_byte = char_to_byte(&subject, (start - 1) as usize);
    let mut found: Option<(usize, usize)> = None;
    for_each_match(&re, &subject, start_byte, |n, s, e, _| {
        if n as i32 == want_n {
            found = Some((s, e));
        }
    });
    if let Some((s, e)) = found { PointerGetDatum(cstring_to_text(&subject[s..e]).cast::<u8>()) } else {
        fcinfo.isnull = true;
        Datum(0)
    }
}

macro_rules! regexp_substr_alias {
    ($name:ident) => {
        #[doc = concat!("PG `", stringify!($name), "` (arity variant of regexp_substr).")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            regexp_substr(fcinfo)
        }
    };
}
regexp_substr_alias!(regexp_substr_no_start);
regexp_substr_alias!(regexp_substr_no_n);
regexp_substr_alias!(regexp_substr_no_flags);
regexp_substr_alias!(regexp_substr_no_subexpr);

/// PG `textregexsubstr`: `substring(text FROM pattern)` -- returns the whole
/// match, or the first capture group if the pattern has one.
pub fn textregexsubstr(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let subject = text_arg_string(fcinfo, 0);
    let pattern = text_arg_string(fcinfo, 1);
    let _ = PG_GET_COLLATION(fcinfo);
    let re = compile(&pattern, RegComp::ADVANCED);
    let Some(caps) = re.exec(&subject, 0) else {
        fcinfo.isnull = true;
        return Datum(0);
    };
    // Group 1 if the pattern captured, else group 0.
    let sel = if re.ngroups() >= 1 { caps.get(1).copied().flatten() } else { caps.first().copied().flatten() };
    if let Some((s, e)) = sel { PointerGetDatum(cstring_to_text(&subject[s..e]).cast::<u8>()) } else {
        fcinfo.isnull = true;
        Datum(0)
    }
}

// A no-op to keep `text` imported for the documented public-signature intent
// (result builders return `*mut text`). Referenced by doc-tests only.
#[allow(dead_code, reason = "keeps `text` in scope for result-builder signatures")]
type TextPtr = *mut text;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::C_COLLATION_OID;
    use crate::postgres::{DatumGetBool, DatumGetCString, DatumGetInt32, NullableDatum};

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

    fn text_out(d: Datum) -> String {
        let out = super::super::varlena::textout(&mut fc(&[d]));
        let p = DatumGetCString(out);
        // SAFETY: textout returns a valid NUL-terminated cstring.
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }

    #[test]
    fn textregexeq_true_false() {
        assert!(DatumGetBool(textregexeq(&mut fc(&[text_datum("abc123"), text_datum("[0-9]+")]))));
        assert!(!DatumGetBool(textregexeq(&mut fc(&[text_datum("abcdef"), text_datum("[0-9]+")]))));
        // `.*asdf.*` -- the primary `name` test shape.
        assert!(DatumGetBool(textregexeq(&mut fc(&[text_datum("xxasdfyy"), text_datum(".*asdf.*")]))));
        // negation operator
        assert!(DatumGetBool(textregexne(&mut fc(&[text_datum("abc"), text_datum("^z")]))));
    }

    #[test]
    fn texticregexeq_case_insensitive() {
        assert!(DatumGetBool(texticregexeq(&mut fc(&[text_datum("ABC"), text_datum("abc")]))));
        assert!(!DatumGetBool(texticregexeq(&mut fc(&[text_datum("ABC"), text_datum("xyz")]))));
    }

    #[test]
    fn nameregexeq_works() {
        // `name` args come through DatumGetName; emulate with a NameData.
        use crate::c::NameData;
        use crate::postgres::PointerGetDatum as pgd;
        let mut nd = NameData { data: [0u8; 64] };
        for (i, b) in b"myrelation".iter().enumerate() {
            nd.data[i] = *b;
        }
        let boxed = Box::leak(Box::new(nd));
        let d = pgd(std::ptr::from_mut(boxed).cast::<u8>());
        assert!(DatumGetBool(nameregexeq(&mut fc(&[d, text_datum("rel")]))));
    }

    #[test]
    fn regexp_replace_backref() {
        // Swap two captured groups via \2\1.
        let d = textregexreplace(&mut fc(&[
            text_datum("John Smith"),
            text_datum("(\\w+)\\s+(\\w+)"),
            text_datum("\\2 \\1"),
            text_datum(""),
        ]));
        assert_eq!(text_out(d), "Smith John");
    }

    #[test]
    fn regexp_replace_global_and_first() {
        // Global replaces every match.
        let d = textregexreplace(&mut fc(&[
            text_datum("a1b2c3"),
            text_datum("[0-9]"),
            text_datum("X"),
            text_datum("g"),
        ]));
        assert_eq!(text_out(d), "aXbXcX");
        // No `g`: only the first match.
        let d = textregexreplace_noopt(&mut fc(&[
            text_datum("a1b2c3"),
            text_datum("[0-9]"),
            text_datum("X"),
        ]));
        assert_eq!(text_out(d), "aXb2c3");
    }

    #[test]
    fn regexp_replace_whole_match_amp() {
        let d = textregexreplace(&mut fc(&[
            text_datum("hello"),
            text_datum("l+"),
            text_datum("[\\&]"),
            text_datum(""),
        ]));
        assert_eq!(text_out(d), "he[ll]o");
    }

    #[test]
    fn regexp_count_and_substr() {
        let c = regexp_count_no_start(&mut fc(&[text_datum("a1b2c3"), text_datum("[0-9]")]));
        assert_eq!(DatumGetInt32(c), 3);
        let mut f = fc(&[text_datum("foo123bar"), text_datum("[0-9]+")]);
        let s = regexp_substr_no_start(&mut f);
        assert!(!f.isnull);
        assert_eq!(text_out(s), "123");
    }

    #[test]
    fn regexp_instr_position() {
        let p = regexp_instr_no_n(&mut fc(&[
            text_datum("abc123"),
            text_datum("[0-9]+"),
            Int32GetDatum(1),
        ]));
        assert_eq!(DatumGetInt32(p), 4); // 1-based char position of '1'
    }

    #[test]
    fn regexp_like_bool() {
        assert!(DatumGetBool(regexp_like_no_flags(&mut fc(&[
            text_datum("abc"),
            text_datum("b"),
        ]))));
    }

    #[test]
    fn unknown_flag_errors() {
        // parse_re_flags raises on 'z'; catch via std::panic (ereport! unwinds).
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parse_re_flags(Some("z"));
        }));
        assert!(r.is_err());
    }

    #[test]
    fn fmgr_table_binds_textregexeq() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "textregexeq")
            .expect("textregexeq present");
        let func = entry.func.expect("textregexeq bound");
        let mut f = fc(&[text_datum("abc123"), text_datum("[0-9]+")]);
        assert!(DatumGetBool(func(&mut f)));
    }
}
