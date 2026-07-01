//! Translated from PostgreSQL src/backend/utils/adt/formatting.c
//!
//! TO_CHAR / TO_TIMESTAMP / TO_DATE / TO_NUMBER: Oracle-inspired picture-based
//! formatting for date/time, interval, and numeric values.
//!
//! Design notes (Rust port):
//!   - `FormatNode` is an owned enum holding an `Action`/`Char`/`Separator`/
//!     `Space` variant; `parse_format` returns an owned `Vec<FormatNode>` (no
//!     fixed-size cache buffers, no raw `*mut`). We re-parse the picture on each
//!     call rather than maintaining PG's global aging cache -- the C global
//!     `DCHCache`/`NUMCache` machinery is intentionally not ported.
//!   - String building uses an owned `String` accumulator instead of C's
//!     `char *s` cursor.
//!
//! STAGED (unimplemented! with a note, or English-only fallback):
//!   - Locale-dependent month/day names (TM prefix): we use the C English
//!     tables. The collation/encoding-aware `localized_*` path is `TODO(locale)`.
//!   - Roman numerals output/input (RM/rm, RN/rn), the EEEE scientific code for
//!     numeric (needs numeric_out_sci wiring), the IS_MULTI ('V') numeric scale,
//!     and dynamic timezone abbreviation parsing (TZ/tz from_char).

use crate::backend::utils::adt::varlena::cstring_to_text;
use crate::datatype::timestamp::{pg_itm, Interval, Timestamp};
use crate::fmgr::FunctionCallInfoBaseData;
use crate::pgtime::pg_tm;
use crate::postgres::{Datum, DatumGetPointer, NullableDatum, PointerGetDatum};
use crate::postgres_ext::InvalidOid;
use crate::utils::datetime::{date2j, j2date};
use crate::utils::elog::ERROR;
use std::fmt::Write as _;

// ===========================================================================
// Constants mirrored from datetime.h / formatting.c
// ===========================================================================

const HOURS_PER_DAY: i64 = 24;
const MONTHS_PER_YEAR: i32 = 12;
const DAYS_PER_MONTH: i32 = 30;
const SECS_PER_HOUR: i64 = 3600;
const SECS_PER_MINUTE: i64 = 60;

const TM_SUFFIX_LEN: usize = 2;
const DCH_MAX_ITEM_SIZ: usize = 12;

const A_D_STR: &str = "A.D.";
const A_D_LOWER: &str = "a.d.";
const AD_STR: &str = "AD";
const AD_LOWER: &str = "ad";
const B_C_STR: &str = "B.C.";
const B_C_LOWER: &str = "b.c.";
const BC_STR: &str = "BC";
const BC_LOWER: &str = "bc";

const A_M_STR: &str = "A.M.";
const A_M_LOWER: &str = "a.m.";
const AM_STR: &str = "AM";
const AM_LOWER: &str = "am";
const P_M_STR: &str = "P.M.";
const P_M_LOWER: &str = "p.m.";
const PM_STR: &str = "PM";
const PM_LOWER: &str = "pm";

/// Full month names (English; TM/locale path is STAGED).
const MONTHS_FULL: [&str; 12] = [
    "January", "February", "March", "April", "May", "June", "July", "August", "September",
    "October", "November", "December",
];
/// Abbreviated month names (matches datetime.rs MONTHS).
const MONTHS_ABBREV: [&str; 12] =
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/// Full day names (matches datetime.rs DAYS).
const DAYS_FULL: [&str; 7] =
    ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const DAYS_SHORT: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

const RM_MONTHS_UPPER: [&str; 12] =
    ["XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I"];
const RM_MONTHS_LOWER: [&str; 12] =
    ["xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i"];

const NUM_TH_UPPER: [&str; 4] = ["ST", "ND", "RD", "TH"];
const NUM_TH_LOWER: [&str; 4] = ["st", "nd", "rd", "th"];

// AD/BC and AM/PM search arrays (even index = AD/AM, odd = BC/PM).
const ADBC_STRINGS: [&str; 4] = [AD_LOWER, BC_LOWER, AD_STR, BC_STR];
const ADBC_STRINGS_LONG: [&str; 4] = [A_D_LOWER, B_C_LOWER, A_D_STR, B_C_STR];
const AMPM_STRINGS: [&str; 4] = [AM_LOWER, PM_LOWER, AM_STR, PM_STR];
const AMPM_STRINGS_LONG: [&str; 4] = [A_M_LOWER, P_M_LOWER, A_M_STR, P_M_STR];

// ===========================================================================
// DCH (date/time) keyword ids
// ===========================================================================

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum Dch {
    A_D,
    A_M,
    AD,
    AM,
    B_C,
    BC,
    CC,
    DAY,
    DDD,
    DD,
    DY,
    Day,
    Dy,
    D,
    FF1,
    FF2,
    FF3,
    FF4,
    FF5,
    FF6,
    FX,
    HH24,
    HH12,
    HH,
    IDDD,
    ID,
    IW,
    IYYY,
    IYY,
    IY,
    I,
    J,
    MI,
    MM,
    MONTH,
    MON,
    MS,
    Month,
    Mon,
    OF,
    P_M,
    PM,
    Q,
    RM,
    SSSS,
    SS,
    TZH,
    TZM,
    TZ,
    US,
    WW,
    W,
    Y_YYY,
    YYYY,
    YYY,
    YY,
    Y,
    a_d,
    a_m,
    p_m,
    ad,
    am,
    b_c,
    bc,
    day,
    dy,
    month,
    mon,
    pm,
    rm,
    tz,
}

/// is this id a "digit" keyword (drives is_next_separator parsing)?
fn dch_is_digit(id: Dch) -> bool {
    matches!(
        id,
        Dch::CC
            | Dch::DDD
            | Dch::DD
            | Dch::D
            | Dch::FF1
            | Dch::FF2
            | Dch::FF3
            | Dch::FF4
            | Dch::FF5
            | Dch::FF6
            | Dch::HH24
            | Dch::HH12
            | Dch::HH
            | Dch::IDDD
            | Dch::ID
            | Dch::IW
            | Dch::IYYY
            | Dch::IYY
            | Dch::IY
            | Dch::I
            | Dch::J
            | Dch::MI
            | Dch::MM
            | Dch::MS
            | Dch::Q
            | Dch::SSSS
            | Dch::SS
            | Dch::TZM
            | Dch::US
            | Dch::WW
            | Dch::W
            | Dch::Y_YYY
            | Dch::YYYY
            | Dch::YYY
            | Dch::YY
            | Dch::Y
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DateMode {
    None,
    Gregorian,
    IsoWeek,
}

fn dch_date_mode(id: Dch) -> DateMode {
    use Dch::{
        D, DAY, DD, DDD, DY, Day, Dy, I, ID, IDDD, IW, IY, IYY, IYYY, MM, MON, MONTH, Mon, Month,
        RM, W, WW, Y, YY, YYY, YYYY, Y_YYY, day, dy, mon, month, rm,
    };
    match id {
        DDD | DD | D | MM | MONTH | MON | Month | Mon | month | mon | RM | rm | WW | W | Y_YYY
        | YYYY | YYY | YY | Y | DAY | DY | Day | Dy | day | dy => DateMode::Gregorian,
        IDDD | ID | IW | IYYY | IYY | IY | I => DateMode::IsoWeek,
        _ => DateMode::None,
    }
}

/// (name, len, id) DCH keyword table -- sorted complicated-to-easy per prefix.
const DCH_KEYWORDS: &[(&str, Dch)] = &[
    ("A.D.", Dch::A_D),
    ("A.M.", Dch::A_M),
    ("AD", Dch::AD),
    ("AM", Dch::AM),
    ("B.C.", Dch::B_C),
    ("BC", Dch::BC),
    ("CC", Dch::CC),
    ("DAY", Dch::DAY),
    ("DDD", Dch::DDD),
    ("DD", Dch::DD),
    ("DY", Dch::DY),
    ("Day", Dch::Day),
    ("Dy", Dch::Dy),
    ("D", Dch::D),
    ("FF1", Dch::FF1),
    ("FF2", Dch::FF2),
    ("FF3", Dch::FF3),
    ("FF4", Dch::FF4),
    ("FF5", Dch::FF5),
    ("FF6", Dch::FF6),
    ("FX", Dch::FX),
    ("HH24", Dch::HH24),
    ("HH12", Dch::HH12),
    ("HH", Dch::HH),
    ("IDDD", Dch::IDDD),
    ("ID", Dch::ID),
    ("IW", Dch::IW),
    ("IYYY", Dch::IYYY),
    ("IYY", Dch::IYY),
    ("IY", Dch::IY),
    ("I", Dch::I),
    ("J", Dch::J),
    ("MI", Dch::MI),
    ("MM", Dch::MM),
    ("MONTH", Dch::MONTH),
    ("MON", Dch::MON),
    ("MS", Dch::MS),
    ("Month", Dch::Month),
    ("Mon", Dch::Mon),
    ("OF", Dch::OF),
    ("P.M.", Dch::P_M),
    ("PM", Dch::PM),
    ("Q", Dch::Q),
    ("RM", Dch::RM),
    ("SSSSS", Dch::SSSS),
    ("SSSS", Dch::SSSS),
    ("SS", Dch::SS),
    ("TZH", Dch::TZH),
    ("TZM", Dch::TZM),
    ("TZ", Dch::TZ),
    ("US", Dch::US),
    ("WW", Dch::WW),
    ("W", Dch::W),
    ("Y,YYY", Dch::Y_YYY),
    ("YYYY", Dch::YYYY),
    ("YYY", Dch::YYY),
    ("YY", Dch::YY),
    ("Y", Dch::Y),
    ("a.d.", Dch::a_d),
    ("a.m.", Dch::a_m),
    ("ad", Dch::ad),
    ("am", Dch::am),
    ("b.c.", Dch::b_c),
    ("bc", Dch::bc),
    ("cc", Dch::CC),
    ("day", Dch::day),
    ("ddd", Dch::DDD),
    ("dd", Dch::DD),
    ("dy", Dch::dy),
    ("d", Dch::D),
    ("ff1", Dch::FF1),
    ("ff2", Dch::FF2),
    ("ff3", Dch::FF3),
    ("ff4", Dch::FF4),
    ("ff5", Dch::FF5),
    ("ff6", Dch::FF6),
    ("fx", Dch::FX),
    ("hh24", Dch::HH24),
    ("hh12", Dch::HH12),
    ("hh", Dch::HH),
    ("iddd", Dch::IDDD),
    ("id", Dch::ID),
    ("iw", Dch::IW),
    ("iyyy", Dch::IYYY),
    ("iyy", Dch::IYY),
    ("iy", Dch::IY),
    ("i", Dch::I),
    ("j", Dch::J),
    ("mi", Dch::MI),
    ("mm", Dch::MM),
    ("month", Dch::month),
    ("mon", Dch::mon),
    ("ms", Dch::MS),
    ("of", Dch::OF),
    ("p.m.", Dch::p_m),
    ("pm", Dch::pm),
    ("q", Dch::Q),
    ("rm", Dch::rm),
    ("sssss", Dch::SSSS),
    ("ssss", Dch::SSSS),
    ("ss", Dch::SS),
    ("tzh", Dch::TZH),
    ("tzm", Dch::TZM),
    ("tz", Dch::tz),
    ("us", Dch::US),
    ("ww", Dch::WW),
    ("w", Dch::W),
    ("y,yyy", Dch::Y_YYY),
    ("yyyy", Dch::YYYY),
    ("yyy", Dch::YYY),
    ("yy", Dch::YY),
    ("y", Dch::Y),
];

// ===========================================================================
// Suffixes
// ===========================================================================

const DCH_S_FM: u8 = 0x01;
const DCH_S_TH: u8 = 0x02;
const DCH_S_TH_LOWER: u8 = 0x04;
const DCH_S_SP: u8 = 0x08;
const DCH_S_TM: u8 = 0x10;

fn s_th_any(s: u8) -> bool {
    (s & DCH_S_TH) != 0 || (s & DCH_S_TH_LOWER) != 0
}
fn s_th_upper(s: u8) -> bool {
    (s & DCH_S_TH) != 0
}
fn s_fm(s: u8) -> bool {
    (s & DCH_S_FM) != 0
}
fn s_tm(s: u8) -> bool {
    (s & DCH_S_TM) != 0
}

// (name, id, is_prefix)
const DCH_SUFF: &[(&str, u8, bool)] = &[
    ("FM", DCH_S_FM, true),
    ("fm", DCH_S_FM, true),
    ("TM", DCH_S_TM, true),
    ("tm", DCH_S_TM, true),
    ("TH", DCH_S_TH, false),
    ("th", DCH_S_TH_LOWER, false),
    ("SP", DCH_S_SP, false),
];

// ===========================================================================
// FormatNode (owned)
// ===========================================================================

#[derive(Clone)]
enum FormatNode {
    Action { id: Dch, suffix: u8 },
    NumAction { id: Num, suffix: u8 },
    Char(String),
    Separator(char),
    Space(char),
}

// ===========================================================================
// parse_format (DCH/DATE-TIME side)
// ===========================================================================

fn is_separator_char(c: char) -> bool {
    let b = c as u32;
    b > 0x20 && b < 0x7F && !c.is_ascii_alphanumeric()
}

/// Find the keyword matching the prefix of `s` (index_seq_search analog).
fn dch_index_seq_search(s: &[u8]) -> Option<(Dch, usize)> {
    let first = *s.first()?;
    if first <= b' ' || first >= b'~' {
        return None;
    }
    for &(name, id) in DCH_KEYWORDS {
        let nb = name.as_bytes();
        if nb[0] == first && s.len() >= nb.len() && &s[..nb.len()] == nb {
            return Some((id, nb.len()));
        }
    }
    None
}

fn suff_search(s: &[u8], want_prefix: bool) -> Option<(u8, usize)> {
    for &(name, id, is_prefix) in DCH_SUFF {
        if is_prefix != want_prefix {
            continue;
        }
        let nb = name.as_bytes();
        if s.len() >= nb.len() && &s[..nb.len()] == nb {
            return Some((id, nb.len()));
        }
    }
    None
}

/// Parse a DATE-TIME format picture into owned FormatNodes.
fn parse_format_dch(str_: &str) -> Vec<FormatNode> {
    let bytes = str_.as_bytes();
    let mut out: Vec<FormatNode> = Vec::with_capacity(bytes.len());
    let mut i = 0usize;

    while i < bytes.len() {
        let mut suffix: u8 = 0;

        // Prefix
        if let Some((id, len)) = suff_search(&bytes[i..], true) {
            suffix |= id;
            i += len;
        }
        if i >= bytes.len() {
            break;
        }

        // Keyword
        if let Some((id, klen)) = dch_index_seq_search(&bytes[i..]) {
            i += klen;
            // Postfix
            if i < bytes.len()
                && let Some((sid, slen)) = suff_search(&bytes[i..], false)
            {
                suffix |= sid;
                i += slen;
            }
            out.push(FormatNode::Action { id, suffix });
        } else {
            // Literal handling. Operate on the char at i.
            let c = str_[i..].chars().next().unwrap_or('\0');
            if c == '"' {
                i += 1;
                while i < bytes.len() {
                    let cc = str_[i..].chars().next().unwrap_or('\0');
                    if cc == '"' {
                        i += 1;
                        break;
                    }
                    if cc == '\\' && i + 1 < bytes.len() {
                        i += 1;
                    }
                    let ch = str_[i..].chars().next().unwrap_or('\0');
                    out.push(FormatNode::Char(ch.to_string()));
                    i += ch.len_utf8();
                }
            } else {
                let mut ch = c;
                if ch == '\\' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 1;
                    ch = '"';
                }
                let chlen = ch.len_utf8();
                if is_separator_char(ch) {
                    out.push(FormatNode::Separator(ch));
                } else if ch.is_whitespace() {
                    out.push(FormatNode::Space(ch));
                } else {
                    out.push(FormatNode::Char(ch.to_string()));
                }
                i += chlen;
            }
        }
    }
    out
}

// ===========================================================================
// TmToChar mirror
// ===========================================================================

#[derive(Default)]
#[allow(
    clippy::struct_field_names,
    reason = "mirrors C struct fmt_tm field names (tm_sec/tm_min/...) for 1:1 fidelity"
)]
struct FmtTm {
    tm_sec: i32,
    tm_min: i32,
    tm_hour: i64,
    tm_mday: i32,
    tm_mon: i32,
    tm_year: i32,
    tm_wday: i32,
    tm_yday: i32,
    tm_gmtoff: i64,
}

struct TmToChar {
    tm: FmtTm,
    fsec: i32,
    tzn: Option<String>,
}

const fn adjust_year(year: i32, is_interval: bool) -> i32 {
    if is_interval {
        year
    } else if year <= 0 {
        -(year - 1)
    } else {
        year
    }
}

// ===========================================================================
// number-to-ordinal helpers (TH/th)
// ===========================================================================

fn get_th(num: &str, upper: bool) -> &'static str {
    let bytes = num.as_bytes();
    let last = if num.len() > 1 && bytes[num.len() - 2] == b'1' {
        0 // all teens get TH
    } else {
        *bytes.last().unwrap_or(&b'0')
    };
    let arr = if upper { &NUM_TH_UPPER } else { &NUM_TH_LOWER };
    match last {
        b'1' => arr[0],
        b'2' => arr[1],
        b'3' => arr[2],
        _ => arr[3],
    }
}

/// Append the ordinal suffix for `num` (the digits already written) to `out`.
fn str_numth(out: &mut String, num: &str, upper: bool) {
    out.push_str(get_th(num, upper));
}

/// Helper: format an integer with optional zero-pad width and TH suffix.
fn emit_int(out: &mut String, val: i64, fm: bool, width: usize, suffix: u8) {
    let body = if fm || width == 0 {
        format!("{val}")
    } else {
        // %0*d semantics: width counts sign too.
        let neg = val < 0;
        let digits = val.unsigned_abs().to_string();
        let mut s = String::new();
        if neg {
            s.push('-');
        }
        let cur = s.len() + digits.len();
        if cur < width {
            for _ in 0..(width - cur) {
                s.push('0');
            }
        }
        s.push_str(&digits);
        s
    };
    out.push_str(&body);
    if s_th_any(suffix) {
        let b = body;
        str_numth(out, &b, s_th_upper(suffix));
    }
}

// pad a name to width with trailing spaces (PG uses %-9s for full month/day).
fn emit_name_padded(out: &mut String, name: &str, fm: bool, width: usize) {
    out.push_str(name);
    if !fm && name.len() < width {
        for _ in 0..(width - name.len()) {
            out.push(' ');
        }
    }
}

// ===========================================================================
// DCH_to_char (date/time -> string)
// ===========================================================================

#[allow(clippy::too_many_lines, reason = "1:1 port of the DCH_to_char switch")]
fn dch_to_char(nodes: &[FormatNode], is_interval: bool, in_: &TmToChar) -> String {
    let tm = &in_.tm;
    let mut out = String::new();

    for n in nodes {
        let (id, suffix) = match n {
            FormatNode::Char(c) => {
                out.push_str(c);
                continue;
            }
            FormatNode::Separator(c) | FormatNode::Space(c) => {
                out.push(*c);
                continue;
            }
            FormatNode::NumAction { .. } => continue,
            FormatNode::Action { id, suffix } => (*id, *suffix),
        };
        let fm = s_fm(suffix);
        let pm_half = tm.tm_hour.rem_euclid(HOURS_PER_DAY) >= HOURS_PER_DAY / 2;

        match id {
            Dch::A_M | Dch::P_M => out.push_str(if pm_half { P_M_STR } else { A_M_STR }),
            Dch::AM | Dch::PM => out.push_str(if pm_half { PM_STR } else { AM_STR }),
            Dch::a_m | Dch::p_m => out.push_str(if pm_half { P_M_LOWER } else { A_M_LOWER }),
            Dch::am | Dch::pm => out.push_str(if pm_half { PM_LOWER } else { AM_LOWER }),
            Dch::HH | Dch::HH12 => {
                let h = tm.tm_hour % (HOURS_PER_DAY / 2);
                let v = if h == 0 { HOURS_PER_DAY / 2 } else { h };
                let width = if tm.tm_hour >= 0 { 2 } else { 3 };
                emit_int(&mut out, v, fm, width, suffix);
            }
            Dch::HH24 => {
                let width = if tm.tm_hour >= 0 { 2 } else { 3 };
                emit_int(&mut out, tm.tm_hour, fm, width, suffix);
            }
            Dch::MI => {
                let width = if tm.tm_min >= 0 { 2 } else { 3 };
                emit_int(&mut out, i64::from(tm.tm_min), fm, width, suffix);
            }
            Dch::SS => {
                let width = if tm.tm_sec >= 0 { 2 } else { 3 };
                emit_int(&mut out, i64::from(tm.tm_sec), fm, width, suffix);
            }
            Dch::FF1 => emit_fsec(&mut out, in_.fsec / 100_000, 1, suffix),
            Dch::FF2 => emit_fsec(&mut out, in_.fsec / 10_000, 2, suffix),
            Dch::FF3 | Dch::MS => emit_fsec(&mut out, in_.fsec / 1000, 3, suffix),
            Dch::FF4 => emit_fsec(&mut out, in_.fsec / 100, 4, suffix),
            Dch::FF5 => emit_fsec(&mut out, in_.fsec / 10, 5, suffix),
            Dch::FF6 | Dch::US => emit_fsec(&mut out, in_.fsec, 6, suffix),
            Dch::SSSS => {
                let v = tm.tm_hour * SECS_PER_HOUR
                    + i64::from(tm.tm_min) * SECS_PER_MINUTE
                    + i64::from(tm.tm_sec);
                emit_int(&mut out, v, true, 0, suffix);
            }
            Dch::tz => {
                if let Some(z) = &in_.tzn {
                    out.push_str(&z.to_ascii_lowercase());
                }
            }
            Dch::TZ => {
                if let Some(z) = &in_.tzn {
                    out.push_str(z);
                }
            }
            Dch::TZH => {
                let sign = if tm.tm_gmtoff >= 0 { '+' } else { '-' };
                out.push(sign);
                let hh = tm.tm_gmtoff.abs() / SECS_PER_HOUR;
                let _ = write!(out, "{hh:02}");
            }
            Dch::TZM => {
                let mm = (tm.tm_gmtoff.abs() % SECS_PER_HOUR) / SECS_PER_MINUTE;
                let _ = write!(out, "{mm:02}");
            }
            Dch::OF => {
                let sign = if tm.tm_gmtoff >= 0 { '+' } else { '-' };
                out.push(sign);
                let hh = tm.tm_gmtoff.abs() / SECS_PER_HOUR;
                if fm {
                    let _ = write!(out, "{hh}");
                } else {
                    let _ = write!(out, "{hh:02}");
                }
                if tm.tm_gmtoff.abs() % SECS_PER_HOUR != 0 {
                    let mm = (tm.tm_gmtoff.abs() % SECS_PER_HOUR) / SECS_PER_MINUTE;
                    let _ = write!(out, ":{mm:02}");
                }
            }
            Dch::A_D | Dch::B_C => out.push_str(if tm.tm_year <= 0 { B_C_STR } else { A_D_STR }),
            Dch::AD | Dch::BC => out.push_str(if tm.tm_year <= 0 { BC_STR } else { AD_STR }),
            Dch::a_d | Dch::b_c => {
                out.push_str(if tm.tm_year <= 0 { B_C_LOWER } else { A_D_LOWER });
            }
            Dch::ad | Dch::bc => out.push_str(if tm.tm_year <= 0 { BC_LOWER } else { AD_LOWER }),
            Dch::MONTH => {
                if tm.tm_mon != 0 {
                    emit_name_padded(&mut out, &MONTHS_FULL[idx_mon(tm)].to_ascii_uppercase(), fm, 9);
                }
            }
            Dch::Month => {
                if tm.tm_mon != 0 {
                    emit_name_padded(&mut out, MONTHS_FULL[idx_mon(tm)], fm, 9);
                }
            }
            Dch::month => {
                if tm.tm_mon != 0 {
                    emit_name_padded(&mut out, &MONTHS_FULL[idx_mon(tm)].to_ascii_lowercase(), fm, 9);
                }
            }
            Dch::MON => {
                if tm.tm_mon != 0 {
                    out.push_str(&MONTHS_ABBREV[idx_mon(tm)].to_ascii_uppercase());
                }
            }
            Dch::Mon => {
                if tm.tm_mon != 0 {
                    out.push_str(MONTHS_ABBREV[idx_mon(tm)]);
                }
            }
            Dch::mon => {
                if tm.tm_mon != 0 {
                    out.push_str(&MONTHS_ABBREV[idx_mon(tm)].to_ascii_lowercase());
                }
            }
            Dch::MM => {
                let width = if tm.tm_mon >= 0 { 2 } else { 3 };
                emit_int(&mut out, i64::from(tm.tm_mon), fm, width, suffix);
            }
            Dch::DAY => {
                emit_name_padded(&mut out, &DAYS_FULL[wday(tm)].to_ascii_uppercase(), fm, 9);
            }
            Dch::Day => emit_name_padded(&mut out, DAYS_FULL[wday(tm)], fm, 9),
            Dch::day => {
                emit_name_padded(&mut out, &DAYS_FULL[wday(tm)].to_ascii_lowercase(), fm, 9);
            }
            Dch::DY => out.push_str(&DAYS_SHORT[wday(tm)].to_ascii_uppercase()),
            Dch::Dy => out.push_str(DAYS_SHORT[wday(tm)]),
            Dch::dy => out.push_str(&DAYS_SHORT[wday(tm)].to_ascii_lowercase()),
            Dch::DDD => emit_int(&mut out, i64::from(tm.tm_yday), fm, 3, suffix),
            Dch::IDDD => {
                let v = crate::backend::utils::adt::timestamp::date2isoyearday(
                    tm.tm_year, tm.tm_mon, tm.tm_mday,
                );
                emit_int(&mut out, i64::from(v), fm, 3, suffix);
            }
            Dch::DD => emit_int(&mut out, i64::from(tm.tm_mday), fm, 2, suffix),
            Dch::D => emit_int(&mut out, i64::from(tm.tm_wday + 1), true, 0, suffix),
            Dch::ID => {
                let v = if tm.tm_wday == 0 { 7 } else { tm.tm_wday };
                emit_int(&mut out, i64::from(v), true, 0, suffix);
            }
            Dch::WW => emit_int(&mut out, i64::from((tm.tm_yday - 1) / 7 + 1), fm, 2, suffix),
            Dch::IW => {
                let v = crate::backend::utils::adt::timestamp::date2isoweek(
                    tm.tm_year, tm.tm_mon, tm.tm_mday,
                );
                emit_int(&mut out, i64::from(v), fm, 2, suffix);
            }
            Dch::Q => {
                if tm.tm_mon != 0 {
                    emit_int(&mut out, i64::from((tm.tm_mon - 1) / 3 + 1), true, 0, suffix);
                }
            }
            Dch::CC => {
                let i = if is_interval {
                    tm.tm_year / 100
                } else if tm.tm_year > 0 {
                    (tm.tm_year - 1) / 100 + 1
                } else {
                    tm.tm_year / 100 - 1
                };
                if (-99..=99).contains(&i) {
                    let width = if i >= 0 { 2 } else { 3 };
                    emit_int(&mut out, i64::from(i), fm, width, suffix);
                } else {
                    emit_int(&mut out, i64::from(i), true, 0, suffix);
                }
            }
            Dch::Y_YYY => {
                let ay = adjust_year(tm.tm_year, is_interval);
                let thousands = ay / 1000;
                let body = format!("{thousands},{:03}", ay - thousands * 1000);
                out.push_str(&body);
                if s_th_any(suffix) {
                    str_numth(&mut out, &body, s_th_upper(suffix));
                }
            }
            Dch::YYYY | Dch::IYYY => {
                let ay = iso_or_year(id == Dch::YYYY, tm, is_interval);
                let width = if ay >= 0 { 4 } else { 5 };
                emit_int(&mut out, i64::from(ay), fm, width, suffix);
            }
            Dch::YYY | Dch::IYY => {
                let ay = iso_or_year(id == Dch::YYY, tm, is_interval) % 1000;
                let width = if iso_or_year(id == Dch::YYY, tm, is_interval) >= 0 { 3 } else { 4 };
                emit_int(&mut out, i64::from(ay), fm, width, suffix);
            }
            Dch::YY | Dch::IY => {
                let ay = iso_or_year(id == Dch::YY, tm, is_interval) % 100;
                let width = if iso_or_year(id == Dch::YY, tm, is_interval) >= 0 { 2 } else { 3 };
                emit_int(&mut out, i64::from(ay), fm, width, suffix);
            }
            Dch::Y | Dch::I => {
                let ay = iso_or_year(id == Dch::Y, tm, is_interval) % 10;
                emit_int(&mut out, i64::from(ay), true, 0, suffix);
            }
            Dch::RM | Dch::rm => {
                if tm.tm_mon != 0 || tm.tm_year != 0 {
                    let months = if id == Dch::RM { &RM_MONTHS_UPPER } else { &RM_MONTHS_LOWER };
                    let mon = match tm.tm_mon.cmp(&0) {
                        std::cmp::Ordering::Equal => {
                            if tm.tm_year >= 0 { 0 } else { (MONTHS_PER_YEAR - 1) as usize }
                        }
                        std::cmp::Ordering::Less => (-(tm.tm_mon + 1)) as usize,
                        std::cmp::Ordering::Greater => (MONTHS_PER_YEAR - tm.tm_mon) as usize,
                    };
                    let name = months[mon];
                    out.push_str(name);
                    if !fm {
                        for _ in 0..(4usize.saturating_sub(name.len())) {
                            out.push(' ');
                        }
                    }
                }
            }
            Dch::W => emit_int(&mut out, i64::from((tm.tm_mday - 1) / 7 + 1), true, 0, suffix),
            Dch::J => {
                let v = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday);
                emit_int(&mut out, i64::from(v), true, 0, suffix);
            }
            Dch::FX => {}
        }
    }
    out
}

#[allow(clippy::cast_sign_loss, reason = "tm_mon is 1..=12 here")]
fn idx_mon(tm: &FmtTm) -> usize {
    (tm.tm_mon - 1) as usize
}
#[allow(clippy::cast_sign_loss, reason = "tm_wday is 0..=6")]
fn wday(tm: &FmtTm) -> usize {
    tm.tm_wday as usize
}

fn iso_or_year(is_plain: bool, tm: &FmtTm, is_interval: bool) -> i32 {
    if is_plain {
        adjust_year(tm.tm_year, is_interval)
    } else {
        adjust_year(
            crate::backend::utils::adt::timestamp::date2isoyear(tm.tm_year, tm.tm_mon, tm.tm_mday),
            is_interval,
        )
    }
}

fn emit_fsec(out: &mut String, frac: i32, width: usize, suffix: u8) {
    let body = format!("{frac:0width$}");
    out.push_str(&body);
    if s_th_any(suffix) {
        str_numth(out, &body, s_th_upper(suffix));
    }
}

// ===========================================================================
// from_char support (parse input back into fields)
// ===========================================================================

#[derive(Default)]
struct TmFromChar {
    mode: DateModeState,
    hh: i32,
    pm: i32,
    mi: i32,
    ss: i32,
    ssss: i32,
    d: i32,
    dd: i32,
    ddd: i32,
    mm: i32,
    ms: i32,
    year: i32,
    bc: i32,
    ww: i32,
    w: i32,
    cc: i32,
    j: i32,
    us: i32,
    yysz: i32,
    clock: i32,
    tzsign: i32,
    tzh: i32,
    tzm: i32,
    ff: i32,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum DateModeState {
    #[default]
    None,
    Gregorian,
    IsoWeek,
}

const CLOCK_12_HOUR: i32 = 1;

fn adjust_partial_year_to_2020(year: i32) -> i32 {
    if year < 70 {
        year + 2000
    } else if year < 100 {
        year + 1900
    } else if year < 520 {
        year + 2000
    } else if year < 1000 {
        year + 1000
    } else {
        year
    }
}

fn from_char_set_mode(out: &mut TmFromChar, mode: DateMode) -> Result<(), String> {
    let m = match mode {
        DateMode::None => return Ok(()),
        DateMode::Gregorian => DateModeState::Gregorian,
        DateMode::IsoWeek => DateModeState::IsoWeek,
    };
    if out.mode == DateModeState::None {
        out.mode = m;
        Ok(())
    } else if out.mode != m {
        Err("invalid combination of date conventions".to_string())
    } else {
        Ok(())
    }
}

fn from_char_set_int(dest: &mut i32, value: i32, name: &str) -> Result<(), String> {
    if *dest != 0 && *dest != value {
        return Err(format!("conflicting values for \"{name}\" field in formatting string"));
    }
    *dest = value;
    Ok(())
}

/// Is the next node a known non-digit (separator)?
fn is_next_separator(nodes: &[FormatNode], idx: usize) -> bool {
    if let FormatNode::Action { id, suffix } = &nodes[idx] {
        if s_th_any(*suffix) {
            return true;
        }
        let _ = id;
    }
    match nodes.get(idx + 1) {
        Some(FormatNode::Action { id, .. }) => !dch_is_digit(*id),
        Some(FormatNode::Char(c)) => !(c.len() == 1 && c.as_bytes()[0].is_ascii_digit()),
        _ => true,
    }
}

fn strspace_len(s: &[u8]) -> usize {
    s.iter().take_while(|&&b| (b as char).is_whitespace()).count()
}

/// Parse an integer of at most `len` chars from `src` at position `*pos`.
/// Returns chars consumed, or Err.
fn from_char_parse_int_len(
    dest: Option<&mut i32>,
    src: &[u8],
    pos: &mut usize,
    len: usize,
    fm: bool,
    is_next_sep: bool,
    name: &str,
) -> Result<usize, String> {
    let init = *pos;
    *pos += strspace_len(&src[*pos..]);

    let take = len.min(DCH_MAX_ITEM_SIZ);
    let avail = &src[*pos..];
    let copy_end = take.min(avail.len());
    let copy = &avail[..copy_end];

    let result: i64;
    if fm || is_next_sep {
        // slurp from the (post-space) position greedily.
        let (val, used) = parse_signed_prefix(&src[*pos..]);
        if used == 0 {
            return Err(format!("invalid value for \"{name}\""));
        }
        result = val;
        *pos += used;
    } else {
        if copy.len() < take {
            return Err(format!("source string too short for \"{name}\" formatting field"));
        }
        let (val, used) = parse_signed_prefix(copy);
        if used > 0 && used < take {
            return Err(format!("invalid value for \"{name}\""));
        }
        result = val;
        *pos += used;
    }

    if *pos == init {
        return Err(format!("invalid value for \"{name}\""));
    }
    if result < i64::from(i32::MIN) || result > i64::from(i32::MAX) {
        return Err(format!("value for \"{name}\" in source string is out of range"));
    }
    if let Some(d) = dest {
        from_char_set_int(d, result as i32, name)?;
    }
    Ok(*pos - init)
}

/// strtol-like: parse leading optional sign + digits.
fn parse_signed_prefix(s: &[u8]) -> (i64, usize) {
    let mut i = 0;
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let dstart = i;
    let mut val: i64 = 0;
    while i < s.len() && s[i].is_ascii_digit() {
        val = val.saturating_mul(10).saturating_add(i64::from(s[i] - b'0'));
        i += 1;
    }
    if i == dstart {
        return (0, 0);
    }
    (if neg { -val } else { val }, i)
}

/// seq_search_ascii: case-insensitive match of `name` prefix against array.
fn seq_search_ascii(name: &[u8], array: &[&str]) -> (i32, usize) {
    if name.is_empty() {
        return (-1, 0);
    }
    let firstc = name[0].to_ascii_lowercase();
    for (ai, a) in array.iter().enumerate() {
        let ab = a.as_bytes();
        if ab.is_empty() || ab[0].to_ascii_lowercase() != firstc {
            continue;
        }
        let mut p = 1;
        let mut matched = true;
        while p < ab.len() {
            if p >= name.len() || !ab[p].eq_ignore_ascii_case(&name[p]) {
                matched = false;
                break;
            }
            p += 1;
        }
        if matched {
            return (ai as i32, ab.len());
        }
    }
    (-1, 0)
}

fn from_char_seq_search(
    src: &[u8],
    pos: &mut usize,
    array: &[&str],
    name: &str,
) -> Result<i32, String> {
    let (dest, len) = seq_search_ascii(&src[*pos..], array);
    if len == 0 {
        return Err(format!("invalid value for \"{name}\""));
    }
    *pos += len;
    Ok(dest)
}

fn skip_thth(src: &[u8], pos: &mut usize, suffix: u8) {
    if s_th_any(suffix) {
        // each of the two chars is single-byte for st/nd/rd/th
        if *pos < src.len() {
            *pos += 1;
        }
        if *pos < src.len() {
            *pos += 1;
        }
    }
}

/// DCH_from_char: parse `in_` per `nodes` into `out`.
fn dch_from_char(nodes: &[FormatNode], in_: &str, out: &mut TmFromChar) -> Result<(), String> {
    let src = in_.as_bytes();
    let mut pos = 0usize;
    let mut extra_skip: i32 = 0;
    let n = nodes.len();

    let mut ni = 0usize;
    while ni < n && pos < src.len() {
        let node = &nodes[ni];

        // Skip leading whitespace before fields (non-FX mode).
        let is_action = matches!(node, FormatNode::Action { .. });
        if is_action || ni == 0 {
            while pos < src.len() && (src[pos] as char).is_whitespace() {
                pos += 1;
                extra_skip += 1;
            }
        }

        match node {
            FormatNode::Space(_) | FormatNode::Separator(_) => {
                extra_skip -= 1;
                if pos < src.len()
                    && ((src[pos] as char).is_whitespace() || is_separator_char(src[pos] as char))
                {
                    pos += 1;
                    extra_skip += 1;
                }
                ni += 1;
                continue;
            }
            FormatNode::Char(_) => {
                if extra_skip > 0 {
                    extra_skip -= 1;
                } else if pos < src.len() {
                    let ch = in_[pos..].chars().next().unwrap_or('\0');
                    pos += ch.len_utf8();
                }
                ni += 1;
                continue;
            }
            FormatNode::NumAction { .. } => {
                ni += 1;
                continue;
            }
            FormatNode::Action { id, suffix } => {
                let id = *id;
                let suffix = *suffix;
                from_char_set_mode(out, dch_date_mode(id))?;
                dch_from_char_one(id, suffix, src, &mut pos, out)?;
            }
        }

        // Ignore trailing spaces after fields (non-FX).
        extra_skip = 0;
        while pos < src.len() && (src[pos] as char).is_whitespace() {
            pos += 1;
            extra_skip += 1;
        }
        ni += 1;
    }
    Ok(())
}

#[allow(clippy::too_many_lines, reason = "1:1 port of the DCH_from_char switch")]
fn dch_from_char_one(
    id: Dch,
    suffix: u8,
    src: &[u8],
    pos: &mut usize,
    out: &mut TmFromChar,
) -> Result<(), String> {
    let fm = s_fm(suffix);
    let key_len = dch_key_len(id);
    let next_sep = true; // conservatively allow greedy where len mismatches; refined below

    macro_rules! parse_n {
        ($dest:expr, $len:expr) => {{
            let isnext = next_sep;
            from_char_parse_int_len(Some($dest), src, pos, $len, fm, isnext, dch_name(id))?;
        }};
    }

    match id {
        Dch::FX => {}
        Dch::A_M | Dch::P_M | Dch::a_m | Dch::p_m => {
            let v = from_char_seq_search(src, pos, &AMPM_STRINGS_LONG, "AM/PM")?;
            from_char_set_int(&mut out.pm, v % 2, "AM/PM")?;
            out.clock = CLOCK_12_HOUR;
        }
        Dch::AM | Dch::PM | Dch::am | Dch::pm => {
            let v = from_char_seq_search(src, pos, &AMPM_STRINGS, "AM/PM")?;
            from_char_set_int(&mut out.pm, v % 2, "AM/PM")?;
            out.clock = CLOCK_12_HOUR;
        }
        Dch::HH | Dch::HH12 => {
            from_char_parse_int_len(Some(&mut out.hh), src, pos, 2, fm, next_sep, "HH")?;
            out.clock = CLOCK_12_HOUR;
            skip_thth(src, pos, suffix);
        }
        Dch::HH24 => {
            from_char_parse_int_len(Some(&mut out.hh), src, pos, 2, fm, next_sep, "HH24")?;
            skip_thth(src, pos, suffix);
        }
        Dch::MI => {
            parse_n!(&mut out.mi, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::SS => {
            parse_n!(&mut out.ss, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::MS => {
            let len = from_char_parse_int_len(Some(&mut out.ms), src, pos, 3, fm, next_sep, "MS")?;
            out.ms *= if len == 1 { 100 } else if len == 2 { 10 } else { 1 };
            skip_thth(src, pos, suffix);
        }
        Dch::FF1 | Dch::FF2 | Dch::FF3 | Dch::FF4 | Dch::FF5 | Dch::FF6 | Dch::US => {
            let flen: usize = match id {
                Dch::FF1 => 1,
                Dch::FF2 => 2,
                Dch::FF3 => 3,
                Dch::FF4 => 4,
                Dch::FF5 => 5,
                Dch::FF6 | Dch::US => 6,
                _ => unreachable!(),
            };
            if id != Dch::US {
                out.ff = flen as i32;
            }
            let len =
                from_char_parse_int_len(Some(&mut out.us), src, pos, flen, fm, next_sep, "US")?;
            out.us *= match len {
                1 => 100_000,
                2 => 10_000,
                3 => 1000,
                4 => 100,
                5 => 10,
                _ => 1,
            };
            skip_thth(src, pos, suffix);
        }
        Dch::SSSS => {
            parse_n!(&mut out.ssss, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::tz | Dch::TZ | Dch::OF | Dch::TZH => {
            // Numeric-offset parse (OF/TZH style). Dynamic zone abbrevs STAGED.
            parse_tz_offset(id, src, pos, out, fm)?;
        }
        Dch::TZM => {
            if out.tzsign == 0 {
                out.tzsign = 1;
            }
            from_char_parse_int_len(Some(&mut out.tzm), src, pos, 2, fm, next_sep, "TZM")?;
        }
        Dch::A_D | Dch::B_C | Dch::a_d | Dch::b_c => {
            let v = from_char_seq_search(src, pos, &ADBC_STRINGS_LONG, "AD/BC")?;
            from_char_set_int(&mut out.bc, v % 2, "AD/BC")?;
        }
        Dch::AD | Dch::BC | Dch::ad | Dch::bc => {
            let v = from_char_seq_search(src, pos, &ADBC_STRINGS, "AD/BC")?;
            from_char_set_int(&mut out.bc, v % 2, "AD/BC")?;
        }
        Dch::MONTH | Dch::Month | Dch::month => {
            let v = from_char_seq_search(src, pos, &MONTHS_FULL, "MONTH")?;
            from_char_set_int(&mut out.mm, v + 1, "MONTH")?;
        }
        Dch::MON | Dch::Mon | Dch::mon => {
            let v = from_char_seq_search(src, pos, &MONTHS_ABBREV, "MON")?;
            from_char_set_int(&mut out.mm, v + 1, "MON")?;
        }
        Dch::MM => {
            parse_n!(&mut out.mm, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::DAY | Dch::Day | Dch::day => {
            let v = from_char_seq_search(src, pos, &DAYS_FULL, "DAY")?;
            from_char_set_int(&mut out.d, v, "DAY")?;
            out.d += 1;
        }
        Dch::DY | Dch::Dy | Dch::dy => {
            let v = from_char_seq_search(src, pos, &DAYS_SHORT, "DY")?;
            from_char_set_int(&mut out.d, v, "DY")?;
            out.d += 1;
        }
        Dch::DDD => {
            parse_n!(&mut out.ddd, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::IDDD => {
            from_char_parse_int_len(Some(&mut out.ddd), src, pos, 3, fm, next_sep, "IDDD")?;
            skip_thth(src, pos, suffix);
        }
        Dch::DD => {
            parse_n!(&mut out.dd, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::D => {
            parse_n!(&mut out.d, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::ID => {
            from_char_parse_int_len(Some(&mut out.d), src, pos, 1, fm, next_sep, "ID")?;
            out.d += 1;
            if out.d > 7 {
                out.d = 1;
            }
            skip_thth(src, pos, suffix);
        }
        Dch::WW | Dch::IW => {
            parse_n!(&mut out.ww, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::Q => {
            from_char_parse_int_len(None, src, pos, key_len, fm, next_sep, "Q")?;
            skip_thth(src, pos, suffix);
        }
        Dch::CC => {
            parse_n!(&mut out.cc, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::Y_YYY => {
            parse_y_yyy(src, pos, out)?;
            skip_thth(src, pos, suffix);
        }
        Dch::YYYY | Dch::IYYY => {
            parse_n!(&mut out.year, key_len);
            out.yysz = 4;
            skip_thth(src, pos, suffix);
        }
        Dch::YYY | Dch::IYY => {
            let len =
                from_char_parse_int_len(Some(&mut out.year), src, pos, key_len, fm, next_sep, "YYY")?;
            if len < 4 {
                out.year = adjust_partial_year_to_2020(out.year);
            }
            out.yysz = 3;
            skip_thth(src, pos, suffix);
        }
        Dch::YY | Dch::IY => {
            let len =
                from_char_parse_int_len(Some(&mut out.year), src, pos, key_len, fm, next_sep, "YY")?;
            if len < 4 {
                out.year = adjust_partial_year_to_2020(out.year);
            }
            out.yysz = 2;
            skip_thth(src, pos, suffix);
        }
        Dch::Y | Dch::I => {
            let len =
                from_char_parse_int_len(Some(&mut out.year), src, pos, key_len, fm, next_sep, "Y")?;
            if len < 4 {
                out.year = adjust_partial_year_to_2020(out.year);
            }
            out.yysz = 1;
            skip_thth(src, pos, suffix);
        }
        Dch::RM | Dch::rm => {
            let v = from_char_seq_search(src, pos, &RM_MONTHS_LOWER, "RM")?;
            from_char_set_int(&mut out.mm, MONTHS_PER_YEAR - v, "RM")?;
        }
        Dch::W => {
            parse_n!(&mut out.w, key_len);
            skip_thth(src, pos, suffix);
        }
        Dch::J => {
            parse_n!(&mut out.j, key_len);
            skip_thth(src, pos, suffix);
        }
    }
    Ok(())
}

fn parse_tz_offset(
    _id: Dch,
    src: &[u8],
    pos: &mut usize,
    out: &mut TmFromChar,
    fm: bool,
) -> Result<(), String> {
    if *pos < src.len() && (src[*pos] == b'+' || src[*pos] == b'-' || src[*pos] == b' ') {
        out.tzsign = if src[*pos] == b'-' { -1 } else { 1 };
        *pos += 1;
    } else {
        out.tzsign = 1;
    }
    from_char_parse_int_len(Some(&mut out.tzh), src, pos, 2, fm, true, "TZH")?;
    if *pos < src.len() && src[*pos] == b':' {
        *pos += 1;
        from_char_parse_int_len(Some(&mut out.tzm), src, pos, 2, fm, true, "TZM")?;
    }
    Ok(())
}

fn parse_y_yyy(src: &[u8], pos: &mut usize, out: &mut TmFromChar) -> Result<(), String> {
    // Match "%d,%03d": millennia, then comma, then exactly 3 digits.
    let (millennia, used1) = parse_signed_prefix(&src[*pos..]);
    if used1 == 0 {
        return Err("invalid value for \"Y,YYY\"".to_string());
    }
    let mut p = *pos + used1;
    if p >= src.len() || src[p] != b',' {
        return Err("invalid value for \"Y,YYY\"".to_string());
    }
    p += 1;
    let mut years: i64 = 0;
    let ystart = p;
    while p < src.len() && p - ystart < 3 && src[p].is_ascii_digit() {
        years = years * 10 + i64::from(src[p] - b'0');
        p += 1;
    }
    if p - ystart < 3 {
        return Err("invalid value for \"Y,YYY\"".to_string());
    }
    let total = millennia
        .checked_mul(1000)
        .and_then(|m| m.checked_add(years))
        .ok_or_else(|| "value for \"Y,YYY\" in source string is out of range".to_string())?;
    if total < i64::from(i32::MIN) || total > i64::from(i32::MAX) {
        return Err("value for \"Y,YYY\" in source string is out of range".to_string());
    }
    from_char_set_int(&mut out.year, total as i32, "Y,YYY")?;
    out.yysz = 4;
    *pos = p;
    Ok(())
}

fn dch_key_len(id: Dch) -> usize {
    for &(name, kid) in DCH_KEYWORDS {
        if kid == id {
            return name.len();
        }
    }
    1
}
fn dch_name(id: Dch) -> &'static str {
    for &(name, kid) in DCH_KEYWORDS {
        if kid == id {
            return name;
        }
    }
    "?"
}

// ===========================================================================
// do_to_timestamp: TmFromChar -> pg_tm + fsec
// ===========================================================================

const SECS_PER_HOUR_I: i32 = 3600;
const SECS_PER_MINUTE_I: i32 = 60;
const HOURS_PER_DAY_I: i32 = 24;

struct FmtTz {
    has_tz: bool,
    gmtoffset: i32,
}

fn new_pg_tm() -> pg_tm {
    pg_tm {
        sec: 0,
        min: 0,
        hour: 0,
        mday: 1,
        mon: 1,
        year: 0,
        wday: 0,
        yday: 0,
        isdst: 0,
        gmtoff: 0,
        zone: None,
    }
}

fn isleap(y: i32) -> bool {
    (y % 4) == 0 && ((y % 100) != 0 || (y % 400) == 0)
}

#[allow(clippy::too_many_lines, reason = "1:1 port of do_to_timestamp")]
fn do_to_timestamp(
    date_txt: &str,
    fmt: &str,
    std: bool,
) -> Result<(pg_tm, i32, FmtTz, i32), String> {
    let mut out = TmFromChar::default();
    let mut tm = new_pg_tm();
    let mut fsec: i32 = 0;
    let mut tz = FmtTz { has_tz: false, gmtoffset: 0 };

    if !fmt.is_empty() {
        let nodes = parse_format_dch(fmt);
        dch_from_char(&nodes, date_txt, &mut out)?;
    }
    let _ = std;

    if out.ssss != 0 {
        let mut x = out.ssss;
        tm.hour = x / SECS_PER_HOUR_I;
        x %= SECS_PER_HOUR_I;
        tm.min = x / SECS_PER_MINUTE_I;
        x %= SECS_PER_MINUTE_I;
        tm.sec = x;
    }
    if out.ss != 0 {
        tm.sec = out.ss;
    }
    if out.mi != 0 {
        tm.min = out.mi;
    }
    if out.hh != 0 {
        tm.hour = out.hh;
    }

    if out.clock == CLOCK_12_HOUR {
        if tm.hour < 1 || tm.hour > HOURS_PER_DAY_I / 2 {
            return Err(format!("hour \"{}\" is invalid for the 12-hour clock", tm.hour));
        }
        if out.pm != 0 && tm.hour < HOURS_PER_DAY_I / 2 {
            tm.hour += HOURS_PER_DAY_I / 2;
        } else if out.pm == 0 && tm.hour == HOURS_PER_DAY_I / 2 {
            tm.hour = 0;
        }
    }

    if out.year != 0 {
        if out.cc != 0 && out.yysz <= 2 {
            let mut cc = out.cc;
            if out.bc != 0 {
                cc = -cc;
            }
            tm.year = out.year % 100;
            if tm.year != 0 {
                if cc >= 0 {
                    tm.year += (cc - 1) * 100;
                } else {
                    tm.year = (cc + 1) * 100 - tm.year + 1;
                }
            } else {
                tm.year = cc * 100 + i32::from(cc < 0);
            }
        } else {
            tm.year = out.year;
            if out.bc != 0 {
                tm.year = -tm.year;
            }
            if tm.year < 0 {
                tm.year += 1;
            }
        }
    } else if out.cc != 0 {
        let mut cc = out.cc;
        if out.bc != 0 {
            cc = -cc;
        }
        if cc >= 0 {
            tm.year = (cc - 1) * 100 + 1;
        } else {
            tm.year = cc * 100 + 1;
        }
    }

    if out.j != 0 {
        let (y, m, d) = j2date(out.j);
        tm.year = y;
        tm.mon = m;
        tm.mday = d;
    }

    if out.ww != 0 {
        if out.mode == DateModeState::IsoWeek {
            let (y, m, d) = if out.d != 0 {
                crate::backend::utils::adt::timestamp::isoweekdate2date(out.ww, out.d)
            } else {
                crate::backend::utils::adt::timestamp::isoweek2date(out.ww)
            };
            tm.year = y;
            tm.mon = m;
            tm.mday = d;
        } else {
            out.ddd = (out.ww - 1) * 7 + 1;
        }
    }
    if out.w != 0 {
        out.dd = (out.w - 1) * 7 + 1;
    }
    if out.dd != 0 {
        tm.mday = out.dd;
    }
    if out.mm != 0 {
        tm.mon = out.mm;
    }

    if out.ddd != 0 && (tm.mon <= 1 || tm.mday <= 1) {
        if tm.year == 0 && out.bc == 0 {
            return Err("cannot calculate day of year without year information".to_string());
        }
        if out.mode == DateModeState::IsoWeek {
            let j0 = crate::backend::utils::adt::timestamp::isoweek2j(tm.year, 1) - 1;
            let (y, m, d) = j2date(j0 + out.ddd);
            tm.year = y;
            tm.mon = m;
            tm.mday = d;
        } else {
            const YSUM: [[i32; 13]; 2] = [
                [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
                [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
            ];
            let y = &YSUM[usize::from(isleap(tm.year))];
            let mut idx = 1usize;
            while idx <= MONTHS_PER_YEAR as usize {
                if out.ddd <= y[idx] {
                    break;
                }
                idx += 1;
            }
            if idx > MONTHS_PER_YEAR as usize {
                idx = MONTHS_PER_YEAR as usize;
            }
            if tm.mon <= 1 {
                tm.mon = idx as i32;
            }
            if tm.mday <= 1 {
                tm.mday = out.ddd - y[idx - 1];
            }
        }
    }

    if out.ms != 0 {
        fsec += out.ms * 1000;
    }
    if out.us != 0 {
        fsec += out.us;
    }

    // timezone
    if out.tzsign != 0 {
        tz.has_tz = true;
        // gmtoffset = (tzh*60 + tzm)*60 seconds; sign convention is flipped.
        let mut off = (out.tzh * 60 + out.tzm) * 60;
        if out.tzsign > 0 {
            off = -off;
        }
        tz.gmtoffset = off;
    }

    Ok((tm, fsec, tz, out.ff))
}

// ===========================================================================
// NUM (numeric) side
// ===========================================================================

#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
enum Num {
    COMMA,
    DEC,
    ZERO,
    NINE,
    B,
    C,
    D,
    E,
    FM,
    G,
    L,
    MI,
    PL,
    PR,
    RN,
    SG,
    SP,
    S,
    TH,
    V,
    rn,
    th,
}

const NUM_KEYWORDS: &[(&str, Num)] = &[
    (",", Num::COMMA),
    (".", Num::DEC),
    ("0", Num::ZERO),
    ("9", Num::NINE),
    ("EEEE", Num::E),
    ("FM", Num::FM),
    ("G", Num::G),
    ("L", Num::L),
    ("MI", Num::MI),
    ("PL", Num::PL),
    ("PR", Num::PR),
    ("RN", Num::RN),
    ("SG", Num::SG),
    ("SP", Num::SP),
    ("S", Num::S),
    ("TH", Num::TH),
    ("V", Num::V),
    ("B", Num::B),
    ("C", Num::C),
    ("D", Num::D),
    ("eeee", Num::E),
    ("fm", Num::FM),
    ("g", Num::G),
    ("l", Num::L),
    ("mi", Num::MI),
    ("pl", Num::PL),
    ("pr", Num::PR),
    ("rn", Num::rn),
    ("sg", Num::SG),
    ("sp", Num::SP),
    ("s", Num::S),
    ("th", Num::th),
    ("v", Num::V),
    ("b", Num::B),
    ("c", Num::C),
    ("d", Num::D),
];

const NUM_F_DECIMAL: u32 = 1 << 1;
const NUM_F_LDECIMAL: u32 = 1 << 2;
const NUM_F_ZERO: u32 = 1 << 3;
const NUM_F_BLANK: u32 = 1 << 4;
const NUM_F_FILLMODE: u32 = 1 << 5;
const NUM_F_LSIGN: u32 = 1 << 6;
const NUM_F_BRACKET: u32 = 1 << 7;
const NUM_F_MINUS: u32 = 1 << 8;
const NUM_F_PLUS: u32 = 1 << 9;
const NUM_F_ROMAN: u32 = 1 << 10;
const NUM_F_MULTI: u32 = 1 << 11;
const NUM_F_EEEE: u32 = 1 << 14;

const NUM_LSIGN_PRE: i32 = -1;
const NUM_LSIGN_POST: i32 = 1;
const NUM_LSIGN_NONE: i32 = 0;

#[derive(Default)]
struct NumDesc {
    pre: i32,
    post: i32,
    lsign: i32,
    flag: u32,
    pre_lsign_num: i32,
    multi: i32,
    zero_start: i32,
    zero_end: i32,
    need_locale: bool,
}

impl NumDesc {
    fn is(&self, f: u32) -> bool {
        self.flag & f != 0
    }
}

fn num_index_seq_search(s: &[u8]) -> Option<(Num, usize)> {
    let first = *s.first()?;
    if first <= b' ' || first >= b'~' {
        return None;
    }
    for &(name, id) in NUM_KEYWORDS {
        let nb = name.as_bytes();
        if nb[0] == first && s.len() >= nb.len() && &s[..nb.len()] == nb {
            return Some((id, nb.len()));
        }
    }
    None
}

#[allow(clippy::too_many_lines, reason = "1:1 port of NUMDesc_prepare switch")]
fn numdesc_prepare(num: &mut NumDesc, id: Num) -> Result<(), String> {
    if num.is(NUM_F_EEEE) && id != Num::E {
        return Err("\"EEEE\" must be the last pattern used".to_string());
    }
    match id {
        Num::NINE => {
            if num.is(NUM_F_BRACKET) {
                return Err("\"9\" must be ahead of \"PR\"".to_string());
            }
            if num.is(NUM_F_MULTI) {
                num.multi += 1;
            } else if num.is(NUM_F_DECIMAL) {
                num.post += 1;
            } else {
                num.pre += 1;
            }
        }
        Num::ZERO => {
            if num.is(NUM_F_BRACKET) {
                return Err("\"0\" must be ahead of \"PR\"".to_string());
            }
            if !num.is(NUM_F_ZERO) && !num.is(NUM_F_DECIMAL) {
                num.flag |= NUM_F_ZERO;
                num.zero_start = num.pre + 1;
            }
            if num.is(NUM_F_DECIMAL) {
                num.post += 1;
            } else {
                num.pre += 1;
            }
            num.zero_end = num.pre + num.post;
        }
        Num::B => {
            if num.pre == 0 && num.post == 0 && !num.is(NUM_F_ZERO) {
                num.flag |= NUM_F_BLANK;
            }
        }
        Num::D => {
            num.flag |= NUM_F_LDECIMAL;
            num.need_locale = true;
            if num.is(NUM_F_DECIMAL) {
                return Err("multiple decimal points".to_string());
            }
            if num.is(NUM_F_MULTI) {
                return Err("cannot use \"V\" and decimal point together".to_string());
            }
            num.flag |= NUM_F_DECIMAL;
        }
        Num::DEC => {
            if num.is(NUM_F_DECIMAL) {
                return Err("multiple decimal points".to_string());
            }
            if num.is(NUM_F_MULTI) {
                return Err("cannot use \"V\" and decimal point together".to_string());
            }
            num.flag |= NUM_F_DECIMAL;
        }
        Num::FM => num.flag |= NUM_F_FILLMODE,
        Num::S => {
            if num.is(NUM_F_LSIGN) {
                return Err("cannot use \"S\" twice".to_string());
            }
            if num.is(NUM_F_PLUS) || num.is(NUM_F_MINUS) || num.is(NUM_F_BRACKET) {
                return Err("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together".to_string());
            }
            if !num.is(NUM_F_DECIMAL) {
                num.lsign = NUM_LSIGN_PRE;
                num.pre_lsign_num = num.pre;
                num.need_locale = true;
                num.flag |= NUM_F_LSIGN;
            } else if num.lsign == NUM_LSIGN_NONE {
                num.lsign = NUM_LSIGN_POST;
                num.need_locale = true;
                num.flag |= NUM_F_LSIGN;
            }
        }
        Num::MI => {
            if num.is(NUM_F_LSIGN) {
                return Err("cannot use \"S\" and \"MI\" together".to_string());
            }
            num.flag |= NUM_F_MINUS;
        }
        Num::PL => {
            if num.is(NUM_F_LSIGN) {
                return Err("cannot use \"S\" and \"PL\" together".to_string());
            }
            num.flag |= NUM_F_PLUS;
        }
        Num::SG => {
            if num.is(NUM_F_LSIGN) {
                return Err("cannot use \"S\" and \"SG\" together".to_string());
            }
            num.flag |= NUM_F_MINUS | NUM_F_PLUS;
        }
        Num::PR => {
            if num.is(NUM_F_LSIGN) || num.is(NUM_F_PLUS) || num.is(NUM_F_MINUS) {
                return Err("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together".to_string());
            }
            num.flag |= NUM_F_BRACKET;
        }
        Num::RN | Num::rn => {
            if num.is(NUM_F_ROMAN) {
                return Err("cannot use \"RN\" twice".to_string());
            }
            num.flag |= NUM_F_ROMAN;
        }
        Num::L | Num::G => num.need_locale = true,
        Num::V => {
            if num.is(NUM_F_DECIMAL) {
                return Err("cannot use \"V\" and decimal point together".to_string());
            }
            num.flag |= NUM_F_MULTI;
        }
        Num::E => {
            if num.is(NUM_F_EEEE) {
                return Err("cannot use \"EEEE\" twice".to_string());
            }
            num.flag |= NUM_F_EEEE;
        }
        Num::COMMA | Num::C | Num::TH | Num::th | Num::SP => {}
    }
    Ok(())
}

/// Parse a NUMBER format picture into FormatNodes and populate NumDesc.
fn parse_format_num(str_: &str) -> Result<(Vec<FormatNode>, NumDesc), String> {
    let bytes = str_.as_bytes();
    let mut out: Vec<FormatNode> = Vec::with_capacity(bytes.len());
    let mut num = NumDesc::default();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some((id, klen)) = num_index_seq_search(&bytes[i..]) {
            i += klen;
            numdesc_prepare(&mut num, id)?;
            out.push(FormatNode::NumAction { id, suffix: 0 });
        } else {
            let c = str_[i..].chars().next().unwrap_or('\0');
            if c == '"' {
                i += 1;
                while i < bytes.len() {
                    let cc = str_[i..].chars().next().unwrap_or('\0');
                    if cc == '"' {
                        i += 1;
                        break;
                    }
                    if cc == '\\' && i + 1 < bytes.len() {
                        i += 1;
                    }
                    let ch = str_[i..].chars().next().unwrap_or('\0');
                    out.push(FormatNode::Char(ch.to_string()));
                    i += ch.len_utf8();
                }
            } else {
                let mut ch = c;
                if ch == '\\' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 1;
                    ch = '"';
                }
                out.push(FormatNode::Char(ch.to_string()));
                i += ch.len_utf8();
            }
        }
    }
    Ok((out, num))
}

/// Last relevant decimal digit position (FM trailing-zero trim).
fn get_last_relevant_decnum(num: &str) -> Option<usize> {
    let dot = num.find('.')?;
    let b = num.as_bytes();
    let mut result = dot;
    let mut p = dot + 1;
    while p < b.len() {
        if b[p] != b'0' {
            result = p;
        }
        p += 1;
    }
    Some(result)
}

/// NUMProc state for to_char (output building) and to_number (parsing).
struct NumProc<'a> {
    is_to_char: bool,
    num: &'a mut NumDesc,
    sign: u8,        // '-' or '+'
    sign_wrote: bool,
    num_count: i32,
    num_in: bool,
    num_curr: i32,
    out_pre_spaces: i32,
    read_dec: bool,
    read_post: i32,
    read_pre: i32,
    number: Vec<u8>, // digit string (to_char: rounded magnitude; to_number: accumulator)
    number_p: usize,
    out: String,
    last_relevant: Option<usize>,
    // locale (C default only)
    decimal: &'static str,
    thousands: &'static str,
    currency: &'static str,
    neg_sign: &'static str,
    pos_sign: &'static str,
}

fn num_processor_to_char(
    nodes: &[FormatNode],
    num: &mut NumDesc,
    number: &str,
    out_pre_spaces: i32,
    sign: u8,
) -> String {
    if num.zero_start != 0 {
        num.zero_start -= 1;
    }
    if num.is(NUM_F_EEEE) {
        // EEEE output: numstr already holds the scientific text.
        return number.to_string();
    }
    if num.is(NUM_F_ROMAN) {
        // Roman output STAGED -- numstr already holds roman or '###'.
        return number.to_string();
    }

    let mut np = NumProc {
        is_to_char: true,
        num,
        sign,
        sign_wrote: false,
        num_count: 0,
        num_in: false,
        num_curr: 0,
        out_pre_spaces,
        read_dec: false,
        read_post: 0,
        read_pre: 0,
        number: number.as_bytes().to_vec(),
        number_p: 0,
        out: String::new(),
        last_relevant: None,
        decimal: ".",
        thousands: ",",
        currency: " ",
        neg_sign: "-",
        pos_sign: "+",
    };

    // sign handling
    if np.num.is(NUM_F_PLUS) || np.num.is(NUM_F_MINUS) {
        np.sign_wrote = !np.num.is(NUM_F_PLUS) || np.num.is(NUM_F_MINUS);
    } else {
        if np.sign != b'-' && np.num.is(NUM_F_FILLMODE) {
            np.num.flag &= !NUM_F_BRACKET;
        }
        np.sign_wrote =
            np.sign == b'+' && np.num.is(NUM_F_FILLMODE) && !np.num.is(NUM_F_LSIGN);
        if np.num.lsign == NUM_LSIGN_PRE && np.num.pre == np.num.pre_lsign_num {
            np.num.lsign = NUM_LSIGN_POST;
        }
    }

    np.num_count = np.num.post + np.num.pre - 1;
    if np.num.is(NUM_F_FILLMODE) && np.num.is(NUM_F_DECIMAL) {
        np.last_relevant = get_last_relevant_decnum(number);
        if let Some(lr) = np.last_relevant
            && np.num.zero_end > np.out_pre_spaces
        {
            let last_zero_pos = number
                .len()
                .saturating_sub(1)
                .min((np.num.zero_end - np.out_pre_spaces).max(0) as usize);
            if lr < last_zero_pos {
                np.last_relevant = Some(last_zero_pos);
            }
        }
    }
    if !np.sign_wrote && np.out_pre_spaces == 0 {
        np.num_count += 1;
    }

    np.num_in = false;
    np.num_curr = 0;
    np.number_p = 0;

    num_processor_loop(&mut np, nodes);
    np.out
}

fn is_predec_space(np: &NumProc) -> bool {
    !np.num.is(NUM_F_ZERO)
        && np.number_p == 0
        && np.number.first() == Some(&b'0')
        && np.num.post != 0
}

fn num_numpart_to_char(np: &mut NumProc, id: Num) {
    if np.num.is(NUM_F_ROMAN) {
        return;
    }
    np.num_in = false;

    if !np.sign_wrote
        && (np.num_curr >= np.out_pre_spaces
            || (np.num.is(NUM_F_ZERO) && np.num.zero_start == np.num_curr))
        && (!is_predec_space(np) || np.last_relevant.is_some_and(|lr| np.number[lr] == b'.'))
    {
        if np.num.is(NUM_F_LSIGN) {
            if np.num.lsign == NUM_LSIGN_PRE {
                np.out.push_str(if np.sign == b'-' { np.neg_sign } else { np.pos_sign });
                np.sign_wrote = true;
            }
        } else if np.num.is(NUM_F_BRACKET) {
            np.out.push(if np.sign == b'+' { ' ' } else { '<' });
            np.sign_wrote = true;
        } else if np.sign == b'+' {
            if !np.num.is(NUM_F_FILLMODE) {
                np.out.push(' ');
            }
            np.sign_wrote = true;
        } else if np.sign == b'-' {
            np.out.push('-');
            np.sign_wrote = true;
        }
    }

    if matches!(id, Num::NINE | Num::ZERO | Num::D | Num::DEC) {
        if np.num_curr < np.out_pre_spaces
            && (np.num.zero_start > np.num_curr || !np.num.is(NUM_F_ZERO))
        {
            if !np.num.is(NUM_F_FILLMODE) {
                np.out.push(' ');
            }
        } else if np.num.is(NUM_F_ZERO)
            && np.num_curr < np.out_pre_spaces
            && np.num.zero_start <= np.num_curr
        {
            np.out.push('0');
            np.num_in = true;
        } else if np.number_p < np.number.len() && np.number[np.number_p] == b'.' {
            let lr_is_dot = np.last_relevant.is_some_and(|lr| np.number[lr] == b'.');
            if !lr_is_dot || np.num.is(NUM_F_FILLMODE) {
                np.out.push_str(np.decimal);
            }
            np.number_p += 1;
        } else {
            // write digit
            let beyond_lr =
                np.last_relevant.is_some_and(|lr| np.number_p > lr) && id != Num::ZERO;
            if beyond_lr {
                // skip
            } else if is_predec_space(np) {
                if !np.num.is(NUM_F_FILLMODE) {
                    np.out.push(' ');
                } else if np.last_relevant.is_some_and(|lr| np.number[lr] == b'.') {
                    np.out.push('0');
                }
            } else if np.number_p < np.number.len() {
                np.out.push(np.number[np.number_p] as char);
                np.num_in = true;
            }
            if np.number_p < np.number.len() {
                np.number_p += 1;
            }
        }

        let end = if np.last_relevant == Some(np.number_p) {
            np.num_curr
        } else {
            np.num_count
                + i32::from(np.out_pre_spaces != 0)
                + i32::from(np.num.is(NUM_F_DECIMAL))
        };
        if np.num_curr + 1 == end {
            if np.sign_wrote && np.num.is(NUM_F_BRACKET) {
                np.out.push(if np.sign == b'+' { ' ' } else { '>' });
            } else if np.num.is(NUM_F_LSIGN) && np.num.lsign == NUM_LSIGN_POST {
                np.out.push_str(if np.sign == b'-' { np.neg_sign } else { np.pos_sign });
            }
        }
    }
    np.num_curr += 1;
}

fn num_processor_loop(np: &mut NumProc, nodes: &[FormatNode]) {
    for n in nodes {
        match n {
            FormatNode::NumAction { id, .. } => {
                let id = *id;
                match id {
                    Num::NINE | Num::ZERO | Num::DEC | Num::D => {
                        num_numpart_to_char(np, id);
                    }
                    Num::COMMA => {
                        if np.num_in {
                            np.out.push(',');
                        } else if !np.num.is(NUM_F_FILLMODE) {
                            np.out.push(' ');
                        }
                    }
                    Num::G => {
                        if np.num_in {
                            np.out.push_str(np.thousands);
                        } else if !np.num.is(NUM_F_FILLMODE) {
                            for _ in 0..np.thousands.len() {
                                np.out.push(' ');
                            }
                        }
                    }
                    Num::L => {
                        np.out.push_str(np.currency);
                    }
                    Num::MI => {
                        if np.sign == b'-' {
                            np.out.push('-');
                        } else if !np.num.is(NUM_F_FILLMODE) {
                            np.out.push(' ');
                        }
                    }
                    Num::PL => {
                        if np.sign == b'+' {
                            np.out.push('+');
                        } else if !np.num.is(NUM_F_FILLMODE) {
                            np.out.push(' ');
                        }
                    }
                    Num::SG => np.out.push(np.sign as char),
                    Num::TH | Num::th
                        if !(np.num.is(NUM_F_ROMAN)
                            || np.number.first() == Some(&b'#')
                            || np.sign == b'-'
                            || np.num.is(NUM_F_DECIMAL)) =>
                    {
                        let numstr = String::from_utf8_lossy(&np.number).into_owned();
                        np.out.push_str(get_th(&numstr, id == Num::TH));
                    }
                    // RN/rn: Roman output STAGED (handled pre-loop). Others: no-op.
                    _ => {}
                }
            }
            FormatNode::Char(c) => {
                np.out.push_str(c);
            }
            FormatNode::Separator(c) | FormatNode::Space(c) => {
                np.out.push(*c);
            }
            FormatNode::Action { .. } => {}
        }
    }
}

// to_number: parse `inout` per nodes, return digit string + sign + scale.
struct NumFromResult {
    number: String, // signed decimal e.g. "-1234.50"
    post: i32,
}

fn num_processor_from_char(
    nodes: &[FormatNode],
    num: &mut NumDesc,
    inout: &str,
) -> Result<NumFromResult, String> {
    if num.zero_start != 0 {
        num.zero_start -= 1;
    }
    if num.is(NUM_F_EEEE) {
        return Err("\"EEEE\" not supported for input".to_string());
    }
    if num.is(NUM_F_ROMAN) {
        return Err("Roman numeral input is not supported".to_string());
    }

    let src = inout.as_bytes();
    let input_len = src.len();
    let mut pos = 0usize;
    // number buffer: first byte is sign slot (' '), then digits/'.'
    let mut number: Vec<u8> = vec![b' '];
    let mut read_dec = false;
    let mut read_post = 0i32;
    let mut read_pre = 0i32;

    for n in nodes {
        if pos >= input_len {
            break;
        }
        match n {
            FormatNode::NumAction { id, .. } => {
                let id = *id;
                match id {
                    Num::NINE | Num::ZERO | Num::DEC | Num::D => {
                        num_numpart_from_char(
                            &mut number,
                            &mut pos,
                            src,
                            input_len,
                            id,
                            num,
                            &mut read_dec,
                            &mut read_post,
                            &mut read_pre,
                        );
                    }
                    Num::COMMA => {
                        if pos < input_len && src[pos] == b',' {
                            pos += 1;
                        }
                    }
                    Num::G => {
                        let ts = b",";
                        if pos + ts.len() <= input_len && &src[pos..pos + ts.len()] == ts {
                            pos += ts.len();
                        }
                    }
                    Num::L => {
                        // eat one non-data char
                        if pos < input_len && !b"0123456789.,+-".contains(&src[pos]) {
                            pos += 1;
                        }
                    }
                    Num::MI => {
                        if pos < input_len && src[pos] == b'-' {
                            number[0] = b'-';
                            pos += 1;
                        }
                    }
                    Num::PL => {
                        if pos < input_len && src[pos] == b'+' {
                            number[0] = b'+';
                            pos += 1;
                        }
                    }
                    Num::SG if pos < input_len && (src[pos] == b'-' || src[pos] == b'+') => {
                        number[0] = src[pos];
                        pos += 1;
                    }
                    _ => {}
                }
            }
            FormatNode::Char(_) | FormatNode::Separator(_) | FormatNode::Space(_) => {
                if pos < input_len {
                    pos += 1;
                }
            }
            FormatNode::Action { .. } => {}
        }
    }

    num.post = read_post;
    // Build signed numeric string.
    let sign = number[0];
    let mut s = String::new();
    if sign == b'-' {
        s.push('-');
    }
    let digits = &number[1..];
    let mut tail = String::from_utf8_lossy(digits).into_owned();
    if tail.ends_with('.') {
        tail.pop();
    }
    if tail.is_empty() {
        tail.push('0');
    }
    s.push_str(&tail);
    Ok(NumFromResult { number: s, post: read_post })
}

#[allow(clippy::too_many_arguments, reason = "1:1 port of NUM_numpart_from_char")]
fn num_numpart_from_char(
    number: &mut Vec<u8>,
    pos: &mut usize,
    src: &[u8],
    input_len: usize,
    id: Num,
    num: &NumDesc,
    read_dec: &mut bool,
    read_post: &mut i32,
    read_pre: &mut i32,
) {
    if *pos >= input_len {
        return;
    }
    if src[*pos] == b' ' {
        *pos += 1;
    }
    if *pos >= input_len {
        return;
    }

    // read sign before number
    if number[0] == b' ' && matches!(id, Num::ZERO | Num::NINE) && (*read_pre + *read_post) == 0 {
        if src[*pos] == b'-' || (num.is(NUM_F_BRACKET) && src[*pos] == b'<') {
            number[0] = b'-';
            *pos += 1;
        } else if src[*pos] == b'+' {
            number[0] = b'+';
            *pos += 1;
        }
    }
    if *pos >= input_len {
        return;
    }

    let mut isread = false;
    if src[*pos].is_ascii_digit() {
        if *read_dec && *read_post == num.post {
            return;
        }
        number.push(src[*pos]);
        if *read_dec {
            *read_post += 1;
        } else {
            *read_pre += 1;
        }
        isread = true;
    } else if num.is(NUM_F_DECIMAL) && !*read_dec && src[*pos] == b'.' {
        number.push(b'.');
        *read_dec = true;
        isread = true;
    }

    if *pos >= input_len {
        return;
    }

    // read sign behind last number (simple + - only; locale STAGED)
    if number[0] == b' '
        && (*read_pre + *read_post) > 0
        && !isread
        && (num.is(NUM_F_PLUS) || num.is(NUM_F_MINUS))
        && (src[*pos] == b'-' || src[*pos] == b'+')
    {
        number[0] = src[*pos];
    }
    let _ = isread;
    *pos += 1;
}

// ===========================================================================
// fmgr glue: arg access + entry points
// ===========================================================================

#[inline]
fn arg_text(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetPointer(fcinfo.args[n].value);
    if p.is_null() {
        return String::new();
    }
    // SAFETY: arg is a valid non-toasted text the caller keeps alive.
    let t = unsafe { &*p.cast::<crate::c::text>() };
    crate::backend::utils::adt::varlena::text_to_cstring(t)
}

#[inline]
fn return_text(s: &str) -> Datum {
    PointerGetDatum(cstring_to_text(s).cast::<u8>())
}

#[inline]
fn arg_timestamp(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Timestamp {
    fcinfo.args[n].value.0 as i64
}
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i32 {
    fcinfo.args[n].value.0 as i32
}
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i64 {
    fcinfo.args[n].value.0 as i64
}
#[inline]
fn arg_float8(fcinfo: &FunctionCallInfoBaseData, n: usize) -> f64 {
    f64::from_bits(fcinfo.args[n].value.0 as u64)
}
#[inline]
fn arg_float4(fcinfo: &FunctionCallInfoBaseData, n: usize) -> f32 {
    f32::from_bits(fcinfo.args[n].value.0 as u32)
}
#[inline]
#[allow(
    clippy::cast_ptr_alignment,
    reason = "Datum payload is a *mut u8; read_unaligned below tolerates the loose alignment"
)]
fn arg_interval(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Interval {
    // SAFETY: interval is pass-by-reference; the arg Datum is a pointer to it.
    // read_unaligned: the Datum payload is a byte pointer with no alignment guarantee.
    let p = DatumGetPointer(fcinfo.args[n].value).cast::<Interval>();
    unsafe { p.read_unaligned() }
}

fn build_tmtochar_from_timestamp(dt: Timestamp, with_tz: bool) -> Option<TmToChar> {
    use crate::backend::utils::adt::timestamp::timestamp2tm;
    use crate::datatype::timestamp::TIMESTAMP_NOT_FINITE;
    if TIMESTAMP_NOT_FINITE(dt) {
        return None;
    }
    let mut tt = new_pg_tm();
    let mut fsec = 0i32;
    let mut tz = 0i32;
    let rc = if with_tz {
        timestamp2tm(dt, Some(&mut tz), &mut tt, &mut fsec, None, std::ptr::null_mut())
    } else {
        timestamp2tm(dt, None, &mut tt, &mut fsec, None, std::ptr::null_mut())
    };
    if rc != 0 {
        crate::elog!(ERROR, "timestamp out of range".to_string());
    }
    let thisdate = date2j(tt.year, tt.mon, tt.mday);
    tt.wday = (thisdate + 1) % 7;
    tt.yday = thisdate - date2j(tt.year, 1, 1) + 1;
    Some(pgtm_to_tmtc(&tt, fsec, None))
}

fn pgtm_to_tmtc(tt: &pg_tm, fsec: i32, tzn: Option<String>) -> TmToChar {
    TmToChar {
        tm: FmtTm {
            tm_sec: tt.sec,
            tm_min: tt.min,
            tm_hour: i64::from(tt.hour),
            tm_mday: tt.mday,
            tm_mon: tt.mon,
            tm_year: tt.year,
            tm_wday: tt.wday,
            tm_yday: tt.yday,
            tm_gmtoff: tt.gmtoff,
        },
        fsec,
        tzn,
    }
}

/// PG `timestamp_to_char`.
pub fn timestamp_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt = arg_timestamp(fcinfo, 0);
    let fmt = arg_text(fcinfo, 1);
    if fmt.is_empty() {
        fcinfo.isnull = true;
        return Datum(0);
    }
    let Some(tmtc) = build_tmtochar_from_timestamp(dt, false) else {
        fcinfo.isnull = true;
        return Datum(0);
    };
    let nodes = parse_format_dch(&fmt);
    return_text(&dch_to_char(&nodes, false, &tmtc))
}

/// PG `timestamptz_to_char`.
pub fn timestamptz_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt = arg_timestamp(fcinfo, 0);
    let fmt = arg_text(fcinfo, 1);
    if fmt.is_empty() {
        fcinfo.isnull = true;
        return Datum(0);
    }
    let Some(tmtc) = build_tmtochar_from_timestamp(dt, true) else {
        fcinfo.isnull = true;
        return Datum(0);
    };
    let nodes = parse_format_dch(&fmt);
    return_text(&dch_to_char(&nodes, false, &tmtc))
}

/// PG `interval_to_char`.
pub fn interval_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let it = arg_interval(fcinfo, 0);
    let fmt = arg_text(fcinfo, 1);
    if fmt.is_empty() {
        fcinfo.isnull = true;
        return Datum(0);
    }
    let mut itm = pg_itm { usec: 0, sec: 0, min: 0, hour: 0, mday: 0, mon: 0, year: 0 };
    crate::backend::utils::adt::timestamp::interval2itm(it, &mut itm);
    let mut tmtc = TmToChar {
        tm: FmtTm {
            tm_sec: itm.sec,
            tm_min: itm.min,
            tm_hour: itm.hour,
            tm_mday: itm.mday,
            tm_mon: itm.mon,
            tm_year: itm.year,
            tm_wday: 0,
            tm_yday: 0,
            tm_gmtoff: 0,
        },
        fsec: itm.usec,
        tzn: None,
    };
    tmtc.tm.tm_yday =
        (tmtc.tm.tm_year * MONTHS_PER_YEAR + tmtc.tm.tm_mon) * DAYS_PER_MONTH + tmtc.tm.tm_mday;
    let nodes = parse_format_dch(&fmt);
    return_text(&dch_to_char(&nodes, true, &tmtc))
}

/// PG `to_timestamp(text, text)`.
pub fn to_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    use crate::backend::utils::adt::timestamp::tm2timestamp;
    let date_txt = arg_text(fcinfo, 0);
    let fmt = arg_text(fcinfo, 1);
    let (tm, fsec, ftz, _fprec) = match do_to_timestamp(&date_txt, &fmt, false) {
        Ok(v) => v,
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    };
    // Use specified zone, else session zone (we only support GMT here).
    let tz = if ftz.has_tz { ftz.gmtoffset } else { 0 };
    let mut result: Timestamp = 0;
    if tm2timestamp(&tm, fsec, Some(&tz), &mut result) != 0 {
        crate::elog!(ERROR, "timestamp out of range".to_string());
    }
    Datum(result as usize)
}

/// PG `to_date(text, text)`.
pub fn to_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    use crate::utils::date::DateADTGetDatum;
    const POSTGRES_EPOCH_JDATE: i32 = 2451545;
    let date_txt = arg_text(fcinfo, 0);
    let fmt = arg_text(fcinfo, 1);
    let (tm, _fsec, _ftz, _fprec) = match do_to_timestamp(&date_txt, &fmt, false) {
        Ok(v) => v,
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    };
    let result = date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;
    DateADTGetDatum(result)
}

// --- numeric rendering via the public fmgr path ---

fn numeric_round_out(value: Datum, post: i32) -> String {
    use crate::backend::utils::adt::numeric::{numeric_out, numeric_round};
    use crate::postgres::Int32GetDatum;
    let mut fc = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: InvalidOid,
        isnull: false,
        nargs: 2,
        args: vec![
            NullableDatum { value, isnull: false },
            NullableDatum { value: Int32GetDatum(post), isnull: false },
        ],
    };
    let rounded = numeric_round(&mut fc);
    let mut fc2 = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: InvalidOid,
        isnull: false,
        nargs: 1,
        args: vec![NullableDatum { value: rounded, isnull: false }],
    };
    let cstr = numeric_out(&mut fc2);
    let p = crate::postgres::DatumGetCString(cstr);
    // SAFETY: numeric_out returns a NUL-terminated C string Datum.
    unsafe { std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned() }
}

fn finish_num_tochar(
    fmt: &str,
    numstr: &str,
    out_pre_spaces: i32,
    sign: u8,
) -> Result<String, String> {
    let (nodes, mut num) = parse_format_num(fmt)?;
    Ok(num_processor_to_char(&nodes, &mut num, numstr, out_pre_spaces, sign))
}

/// PG `numeric_to_char`.
pub fn numeric_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = fcinfo.args[0].value;
    let fmt = arg_text(fcinfo, 1);
    if fmt.is_empty() {
        return return_text("");
    }
    let (_nodes, num) = match parse_format_num(&fmt) {
        Ok(v) => v,
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    };

    if num.is(NUM_F_ROMAN) || num.is(NUM_F_EEEE) || num.is(NUM_F_MULTI) {
        // Roman/EEEE/V scale STAGED for numeric to_char.
        crate::elog!(
            ERROR,
            "to_char: RN/EEEE/V codes are not yet supported for numeric".to_string()
        );
        unreachable!()
    }

    let orgnum = numeric_round_out(value, num.post);
    let (sign, numstr_body) = orgnum.strip_prefix('-').map_or_else(
        || (b'+', orgnum.clone()),
        |stripped| (b'-', stripped.to_string()),
    );

    let pre_len = numstr_body.find('.').unwrap_or(numstr_body.len());
    let mut out_pre_spaces = 0i32;
    let numstr = match pre_len.cmp(&(num.pre as usize)) {
        std::cmp::Ordering::Less => {
            out_pre_spaces = num.pre - pre_len as i32;
            numstr_body
        }
        std::cmp::Ordering::Greater => {
            // overflow -> all '#'
            let total = (num.pre + num.post + 1).max(0) as usize;
            let mut s: String = "#".repeat(total);
            // place '.' at Num.pre position
            if (num.pre as usize) < s.len() {
                let mut bytes = s.into_bytes();
                bytes[num.pre as usize] = b'.';
                s = String::from_utf8(bytes).unwrap_or_default();
            }
            s
        }
        std::cmp::Ordering::Equal => numstr_body,
    };

    match finish_num_tochar(&fmt, &numstr, out_pre_spaces, sign) {
        Ok(s) => return_text(&s),
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    }
}

fn int_to_char_common(fmt: &str, value: i64, num: &NumDesc) -> (String, i32, u8) {
    let mut out_pre_spaces = 0i32;
    let (sign, mut orgnum) = if value < 0 {
        (b'-', value.unsigned_abs().to_string())
    } else {
        (b'+', value.to_string())
    };
    let pre_len = orgnum.len();
    if num.post != 0 {
        orgnum.push('.');
        for _ in 0..num.post {
            orgnum.push('0');
        }
    }
    let numstr = match pre_len.cmp(&(num.pre as usize)) {
        std::cmp::Ordering::Less => {
            out_pre_spaces = num.pre - pre_len as i32;
            orgnum
        }
        std::cmp::Ordering::Greater => {
            let total = (num.pre + num.post + 1).max(0) as usize;
            let mut bytes = "#".repeat(total).into_bytes();
            if (num.pre as usize) < bytes.len() {
                bytes[num.pre as usize] = b'.';
            }
            String::from_utf8(bytes).unwrap_or_default()
        }
        std::cmp::Ordering::Equal => orgnum,
    };
    (numstr, out_pre_spaces, sign)
}

fn int_to_char_entry(fcinfo: &FunctionCallInfoBaseData, value: i64) -> Datum {
    let fmt = arg_text(fcinfo, 1);
    if fmt.is_empty() {
        return return_text("");
    }
    let (_nodes, num) = match parse_format_num(&fmt) {
        Ok(v) => v,
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    };
    if num.is(NUM_F_ROMAN) || num.is(NUM_F_EEEE) || num.is(NUM_F_MULTI) {
        crate::elog!(ERROR, "to_char: RN/EEEE/V codes are not yet supported for int".to_string());
        unreachable!()
    }
    let (numstr, out_pre_spaces, sign) = int_to_char_common(&fmt, value, &num);
    match finish_num_tochar(&fmt, &numstr, out_pre_spaces, sign) {
        Ok(s) => return_text(&s),
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    }
}

/// PG `int4_to_char`.
pub fn int4_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = i64::from(arg_int32(fcinfo, 0));
    int_to_char_entry(fcinfo, value)
}

/// PG `int8_to_char`.
pub fn int8_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = arg_int64(fcinfo, 0);
    int_to_char_entry(fcinfo, value)
}

fn float_to_char_common(value: f64, num: &NumDesc, dig: i32) -> (String, i32, u8) {
    let mut out_pre_spaces = 0i32;
    let mut post = num.post;
    let pre_str = format!("{:.0}", value.abs());
    let pre_count = pre_str.len() as i32;
    if pre_count >= dig {
        post = 0;
    } else if pre_count + post > dig {
        post = dig - pre_count;
    }
    let body = format!("{value:.*}", post.max(0) as usize);
    let (sign, numstr_body) = body
        .strip_prefix('-')
        .map_or_else(|| (b'+', body.clone()), |s| (b'-', s.to_string()));
    let pre_len = numstr_body.find('.').unwrap_or(numstr_body.len());
    let numstr = match pre_len.cmp(&(num.pre as usize)) {
        std::cmp::Ordering::Less => {
            out_pre_spaces = num.pre - pre_len as i32;
            numstr_body
        }
        std::cmp::Ordering::Greater => {
            let total = (num.pre + post + 1).max(0) as usize;
            let mut bytes = "#".repeat(total).into_bytes();
            if (num.pre as usize) < bytes.len() {
                bytes[num.pre as usize] = b'.';
            }
            String::from_utf8(bytes).unwrap_or_default()
        }
        std::cmp::Ordering::Equal => numstr_body,
    };
    (numstr, out_pre_spaces, sign)
}

/// PG `float4_to_char`.
pub fn float4_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = f64::from(arg_float4(fcinfo, 0));
    float_to_char_entry(fcinfo, value, 6) // FLT_DIG
}

/// PG `float8_to_char`.
pub fn float8_to_char(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = arg_float8(fcinfo, 0);
    float_to_char_entry(fcinfo, value, 15) // DBL_DIG
}

fn float_to_char_entry(fcinfo: &FunctionCallInfoBaseData, value: f64, dig: i32) -> Datum {
    let fmt = arg_text(fcinfo, 1);
    if fmt.is_empty() {
        return return_text("");
    }
    let (_nodes, num) = match parse_format_num(&fmt) {
        Ok(v) => v,
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    };
    if num.is(NUM_F_ROMAN) || num.is(NUM_F_EEEE) || num.is(NUM_F_MULTI) {
        crate::elog!(ERROR, "to_char: RN/EEEE/V codes are not yet supported for float".to_string());
        unreachable!()
    }
    let (numstr, out_pre_spaces, sign) = float_to_char_common(value, &num, dig);
    match finish_num_tochar(&fmt, &numstr, out_pre_spaces, sign) {
        Ok(s) => return_text(&s),
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    }
}

/// PG `numeric_to_number(text, text)` -> numeric.
pub fn numeric_to_number(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    use crate::backend::utils::adt::numeric::numeric_in;
    use crate::postgres::{CStringGetDatum, Int32GetDatum, ObjectIdGetDatum};
    let value = arg_text(fcinfo, 0);
    let fmt = arg_text(fcinfo, 1);
    if fmt.is_empty() {
        fcinfo.isnull = true;
        return Datum(0);
    }
    let (nodes, mut num) = match parse_format_num(&fmt) {
        Ok(v) => v,
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    };
    if num.is(NUM_F_MULTI) {
        crate::elog!(ERROR, "to_number: \"V\" scale is not yet supported".to_string());
        unreachable!()
    }
    let res = match num_processor_from_char(&nodes, &mut num, &value) {
        Ok(v) => v,
        Err(e) => {
            crate::elog!(ERROR, e.clone());
            unreachable!()
        }
    };
    let scale = res.post;
    let precision = num.pre + num.multi + scale;
    let typmod = ((precision << 16) | scale) + crate::c::VARHDRSZ;
    let c = std::ffi::CString::new(res.number).unwrap_or_default();
    let mut fc = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: InvalidOid,
        isnull: false,
        nargs: 3,
        args: vec![
            NullableDatum { value: CStringGetDatum(c.into_raw()), isnull: false },
            NullableDatum { value: ObjectIdGetDatum(InvalidOid), isnull: false },
            NullableDatum { value: Int32GetDatum(typmod), isnull: false },
        ],
    };
    numeric_in(&mut fc)
}

// ===========================================================================
// str_tolower / str_toupper / str_initcap and the SQL lower()/upper()/initcap()
// (formatting.c). The default (C/database-encoding) path folds ASCII plus the
// Unicode simple-case mapping; the ICU/locale collation path is staged.
// ===========================================================================

/// Borrow a non-toasted varlena's payload bytes.
///
/// SAFETY: `p` is a valid 4-byte-or-short-header varlena that outlives the borrow.
unsafe fn fmt_varlena_bytes<'a>(p: *mut u8) -> &'a [u8] {
    let len = crate::varatt::VARSIZE_ANY_EXHDR(p);
    core::slice::from_raw_parts(crate::varatt::VARDATA_ANY(p), len)
}

/// PG `str_tolower`: lowercase a UTF-8 buffer (default collation path).
#[must_use]
pub fn str_tolower(s: &str) -> String {
    s.chars().flat_map(char::to_lowercase).collect()
}

/// PG `str_toupper`: uppercase a UTF-8 buffer (default collation path).
#[must_use]
pub fn str_toupper(s: &str) -> String {
    s.chars().flat_map(char::to_uppercase).collect()
}

/// PG `str_initcap`: uppercase the first letter of each word, lowercase the rest.
/// A "word" starts after any non-alphanumeric character (`wasalnum` tracking).
#[must_use]
pub fn str_initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut wasalnum = false;
    for c in s.chars() {
        if wasalnum {
            out.extend(c.to_lowercase());
        } else {
            out.extend(c.to_uppercase());
        }
        wasalnum = c.is_alphanumeric();
    }
    out
}

/// Read the sole text arg as a `String` and apply `f`, returning a text Datum.
fn text_map(fcinfo: &FunctionCallInfoBaseData, f: impl Fn(&str) -> String) -> Datum {
    let p = DatumGetPointer(fcinfo.args[0].value);
    // SAFETY: the arg is a valid non-toasted text varlena.
    let bytes = unsafe { fmt_varlena_bytes(p) };
    let s = String::from_utf8_lossy(bytes);
    let out = f(&s);
    PointerGetDatum(cstring_to_text(&out).cast::<u8>())
}

/// PG `lower`: SQL `lower(text)`.
pub fn lower(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    text_map(fcinfo, str_tolower)
}

/// PG `upper`: SQL `upper(text)`.
pub fn upper(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    text_map(fcinfo, str_toupper)
}

/// PG `initcap`: SQL `initcap(text)`.
pub fn initcap(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    text_map(fcinfo, str_initcap)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{Int32GetDatum, NullableDatum, PointerGetDatum};

    fn mk_text(s: &str) -> Datum {
        PointerGetDatum(cstring_to_text(s).cast::<u8>())
    }

    fn read_text(d: Datum) -> String {
        let p = DatumGetPointer(d);
        let t = unsafe { &*p.cast::<crate::c::text>() };
        crate::backend::utils::adt::varlena::text_to_cstring(t)
    }

    fn fcinfo(args: Vec<Datum>) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: None,
            resultinfo: None,
            fncollation: InvalidOid,
            isnull: false,
            nargs: args.len() as i16,
            args: args.into_iter().map(|value| NullableDatum { value, isnull: false }).collect(),
        }
    }

    // 2024-01-15 13:05:09 as a Timestamp (usec since 2000-01-01).
    fn ts_20240115_130509() -> i64 {
        let jd = date2j(2024, 1, 15);
        let days = i64::from(jd - 2451545);
        days * 86_400_000_000 + 13 * 3_600_000_000 + 5 * 60_000_000 + 9 * 1_000_000
    }

    #[test]
    fn to_char_ymd_hms() {
        let mut fc = fcinfo(vec![Datum(ts_20240115_130509() as usize), mk_text("YYYY-MM-DD HH24:MI:SS")]);
        let d = timestamp_to_char(&mut fc);
        assert_eq!(read_text(d), "2024-01-15 13:05:09");
    }

    #[test]
    fn to_char_month_name() {
        let mut fc = fcinfo(vec![Datum(ts_20240115_130509() as usize), mk_text("Month DD, YYYY")]);
        let d = timestamp_to_char(&mut fc);
        // "Month" is space-padded to 9 chars: "January  "
        assert_eq!(read_text(d), "January   15, 2024");
    }

    #[test]
    fn to_char_12h_pm() {
        let mut fc = fcinfo(vec![Datum(ts_20240115_130509() as usize), mk_text("HH12:MI AM")]);
        let d = timestamp_to_char(&mut fc);
        assert_eq!(read_text(d), "01:05 PM");
    }

    #[test]
    fn to_char_fm_trims() {
        let mut fc = fcinfo(vec![Datum(ts_20240115_130509() as usize), mk_text("FMMonth FMDD, YYYY")]);
        let d = timestamp_to_char(&mut fc);
        assert_eq!(read_text(d), "January 15, 2024");
    }

    #[test]
    fn to_date_roundtrip() {
        let mut fc = fcinfo(vec![mk_text("2024-01-15"), mk_text("YYYY-MM-DD")]);
        let d = to_date(&mut fc);
        let expected = date2j(2024, 1, 15) - 2451545;
        assert_eq!(d.0 as i32, expected);
    }

    #[test]
    fn to_timestamp_roundtrip() {
        let mut fc = fcinfo(vec![mk_text("2024-01-15 10:30"), mk_text("YYYY-MM-DD HH24:MI")]);
        let d = to_timestamp(&mut fc);
        let jd = date2j(2024, 1, 15);
        let days = i64::from(jd - 2451545);
        let expected = days * 86_400_000_000 + 10 * 3_600_000_000 + 30 * 60_000_000;
        assert_eq!(d.0 as i64, expected);
    }

    #[test]
    fn numeric_to_char_basic() {
        // to_char(1234.5, '9999.99') -> "1234.50"
        use crate::backend::utils::adt::numeric::numeric_in;
        use crate::postgres::{CStringGetDatum, ObjectIdGetDatum};
        let c = std::ffi::CString::new("1234.5").unwrap();
        let mut nf = fcinfo(vec![
            CStringGetDatum(c.into_raw()),
            ObjectIdGetDatum(InvalidOid),
            Int32GetDatum(-1),
        ]);
        let numv = numeric_in(&mut nf);
        let mut fc = fcinfo(vec![numv, mk_text("9999.99")]);
        let d = numeric_to_char(&mut fc);
        assert_eq!(read_text(d).trim_start(), "1234.50");
    }

    #[test]
    fn int_to_char_sign() {
        // to_char(-12, 'S999'): S anchors the sign; 999 leaves one unfilled
        // digit position rendered as a space, matching PG's " -12".
        let mut fc = fcinfo(vec![Int32GetDatum(-12), mk_text("S999")]);
        let d = int4_to_char(&mut fc);
        assert_eq!(read_text(d), " -12");
    }

    #[test]
    fn bound_fn_resolves_via_fmgr() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "timestamp_to_char")
            .expect("timestamp_to_char present");
        let func = entry.func.expect("timestamp_to_char bound");
        let mut f = fcinfo(vec![Datum(ts_20240115_130509() as usize), mk_text("YYYY")]);
        assert_eq!(read_text(func(&mut f)), "2024");
    }

    #[test]
    fn to_number_basic() {
        // to_number('1,234.50','9,999.99') -> 1234.50
        use crate::backend::utils::adt::numeric::numeric_out;
        let mut fc = fcinfo(vec![mk_text("1,234.50"), mk_text("9,999.99")]);
        let numv = numeric_to_number(&mut fc);
        let mut of = fcinfo(vec![numv]);
        let outp = numeric_out(&mut of);
        let p = crate::postgres::DatumGetCString(outp);
        let s = unsafe { std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned() };
        assert_eq!(s, "1234.50");
    }
}
