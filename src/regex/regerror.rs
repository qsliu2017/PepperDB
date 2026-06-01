// regerror - error-code expansion
//
// Translated 1:1 from postgres/src/backend/regex/regerror.c with the error
// table merged in from postgres/src/include/regex/regerrs.h. The regex
// subsystem is not yet ported, so this file is self-contained: it defines the
// REG_* error-code constants (from regex.h) it needs and the static rerrs[]
// table, then provides pg_regerror.

use crate::prelude::*;

// Error-reporting codes, copied verbatim from
// postgres/src/include/regex/regex.h. Be careful if modifying -- the rerrs
// table below is keyed on these numeric values.
pub const REG_OKAY: c_int = 0; // no errors detected
pub const REG_NOMATCH: c_int = 1; // failed to match
pub const REG_BADPAT: c_int = 2; // invalid regexp
pub const REG_ECOLLATE: c_int = 3; // invalid collating element
pub const REG_ECTYPE: c_int = 4; // invalid character class
pub const REG_EESCAPE: c_int = 5; // invalid escape \ sequence
pub const REG_ESUBREG: c_int = 6; // invalid backreference number
pub const REG_EBRACK: c_int = 7; // brackets [] not balanced
pub const REG_EPAREN: c_int = 8; // parentheses () not balanced
pub const REG_EBRACE: c_int = 9; // braces {} not balanced
pub const REG_BADBR: c_int = 10; // invalid repetition count(s)
pub const REG_ERANGE: c_int = 11; // invalid character range
pub const REG_ESPACE: c_int = 12; // out of memory
pub const REG_BADRPT: c_int = 13; // quantifier operand invalid
pub const REG_ASSERT: c_int = 15; // "can't happen" -- you found a bug
pub const REG_INVARG: c_int = 16; // invalid argument to regex function
pub const REG_MIXED: c_int = 17; // character widths of regex and string differ
pub const REG_BADOPT: c_int = 18; // invalid embedded option
pub const REG_ETOOBIG: c_int = 19; // regular expression is too complex
pub const REG_ECOLORS: c_int = 20; // too many colors
// two specials for debugging and testing
pub const REG_ATOI: c_int = 101; // convert error-code name to number
pub const REG_ITOA: c_int = 102; // convert error-code number to name
// non-error result codes for pg_regprefix
pub const REG_PREFIX: c_int = -1; // identified a common prefix
pub const REG_EXACT: c_int = -2; // identified an exact match

// unknown-error explanation; %x will be filled in manually below.
const UNK: &str = "*** unknown regex error code 0x";

// struct to map among codes, code names, and explanations
struct Rerr {
    code: c_int,
    name: &'static str,
    explain: &'static str,
}

// The actual table, merged from regex/regerrs.h. The final {-1, "", "oops"}
// row is the sentinel/"unknown" fallback whose explanation is special-cased in
// pg_regerror.
static RERRS: &[Rerr] = &[
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
    // sentinel: explanation special-cased in code
    Rerr { code: -1, name: "", explain: "oops" },
];

// Read a NUL-terminated C string from errbuf into a Rust &str (lossy on the
// off chance of invalid UTF-8). Used by REG_ATOI / REG_ITOA which take their
// input from the caller-provided buffer.
unsafe fn cbuf_to_string(errbuf: *const c_char) -> String {
    let mut len = 0usize;
    while *errbuf.add(len) != 0 {
        len += 1;
    }
    let bytes = core::slice::from_raw_parts(errbuf as *const u8, len);
    String::from_utf8_lossy(bytes).into_owned()
}

// pg_regerror - the interface to error numbers.
//
// Mirrors the C semantics exactly:
//   REG_ATOI: read a code name from errbuf, look it up, return its number as
//             a decimal string ("-1" if unknown).
//   REG_ITOA: read a number (atoi) from errbuf, look it up, return its name
//             ("REG_<n>" if unknown).
//   default:  look up the code, return its explanation (or the "unknown"
//             message if not found).
// Copies at most errbuf_size-1 bytes plus a NUL into errbuf (when
// errbuf_size > 0) and returns the full untruncated length including the NUL.
pub unsafe fn pg_regerror(
    errcode: c_int,
    _preg: *const c_void,
    errbuf: *mut c_char,
    errbuf_size: Size,
) -> Size {
    let msg: String;

    match errcode {
        REG_ATOI => {
            // convert name to number
            let want = cbuf_to_string(errbuf);
            let mut code: c_int = -1;
            // iterate while r->code >= 0 (i.e. skip the sentinel row)
            for r in RERRS.iter().take_while(|r| r.code >= 0) {
                if r.name == want {
                    code = r.code;
                    break;
                }
            }
            // -1 for unknown (matches the value the C loop lands on)
            msg = format!("{}", code);
        }
        REG_ITOA => {
            // convert number to name
            let icode = c_atoi(errbuf);
            let mut found: Option<&'static str> = None;
            for r in RERRS.iter().take_while(|r| r.code >= 0) {
                if r.code == icode {
                    found = Some(r.name);
                    break;
                }
            }
            msg = match found {
                Some(name) => name.to_string(),
                // unknown; tell him the number (C uses %u)
                None => format!("REG_{}", icode as u32),
            };
        }
        _ => {
            // a real, normal error code
            let mut found: Option<&'static str> = None;
            for r in RERRS.iter().take_while(|r| r.code >= 0) {
                if r.code == errcode {
                    found = Some(r.explain);
                    break;
                }
            }
            msg = match found {
                Some(explain) => explain.to_string(),
                // unknown; say so (C: sprintf(convbuf, unk, errcode) with %x)
                None => format!("{}{:x} ***", UNK, errcode),
            };
        }
    }

    let msg_bytes = msg.as_bytes();
    let len: Size = msg_bytes.len() + 1; // space needed, including NUL

    if errbuf_size > 0 {
        if errbuf_size > len {
            // whole string fits, including NUL
            core::ptr::copy_nonoverlapping(
                msg_bytes.as_ptr() as *const c_char,
                errbuf,
                msg_bytes.len(),
            );
            *errbuf.add(msg_bytes.len()) = 0;
        } else {
            // truncate to fit: errbuf_size-1 bytes + NUL
            let n = errbuf_size - 1;
            core::ptr::copy_nonoverlapping(
                msg_bytes.as_ptr() as *const c_char,
                errbuf,
                n,
            );
            *errbuf.add(n) = 0;
        }
    }

    len
}

// Minimal atoi over a NUL-terminated C buffer, matching C's atoi() leniency:
// skip leading whitespace, optional sign, then consume decimal digits and stop
// at the first non-digit. "not our problem if this fails" -> returns 0.
unsafe fn c_atoi(errbuf: *const c_char) -> c_int {
    let mut i = 0usize;
    // skip leading whitespace
    loop {
        let ch = *errbuf.add(i) as u8;
        if ch == b' ' || ch == b'\t' || ch == b'\n' || ch == b'\r'
            || ch == 0x0b || ch == 0x0c
        {
            i += 1;
        } else {
            break;
        }
    }
    let mut neg = false;
    let sign = *errbuf.add(i) as u8;
    if sign == b'+' || sign == b'-' {
        neg = sign == b'-';
        i += 1;
    }
    let mut val: i64 = 0;
    loop {
        let ch = *errbuf.add(i) as u8;
        if ch.is_ascii_digit() {
            val = val * 10 + (ch - b'0') as i64;
            i += 1;
        } else {
            break;
        }
    }
    if neg {
        val = -val;
    }
    val as c_int
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: call pg_regerror with a fixed-size buffer, returning the needed
    // length and the resulting NUL-terminated string contents.
    unsafe fn call(errcode: c_int, input: Option<&str>, bufsize: usize) -> (Size, String) {
        let mut buf = vec![0u8 as c_char; bufsize.max(1)];
        if let Some(s) = input {
            // pre-load the buffer with the input string (for ATOI/ITOA)
            for (k, b) in s.bytes().enumerate() {
                buf[k] = b as c_char;
            }
            buf[s.len()] = 0;
        }
        let len = pg_regerror(
            errcode,
            core::ptr::null(),
            buf.as_mut_ptr(),
            bufsize as Size,
        );
        // read back the C string
        let mut out = Vec::new();
        let mut k = 0usize;
        while k < bufsize && buf[k] != 0 {
            out.push(buf[k] as u8);
            k += 1;
        }
        (len, String::from_utf8_lossy(&out).into_owned())
    }

    #[test]
    fn nomatch_explain_and_length() {
        let expected = "failed to match";
        unsafe {
            let (len, s) = call(REG_NOMATCH, None, 64);
            assert_eq!(s, expected);
            assert_eq!(len, expected.len() + 1);
        }
    }

    #[test]
    fn unknown_code_falls_back() {
        // 9999 is not in the table -> the "unknown" message with hex code
        let expected = format!("*** unknown regex error code 0x{:x} ***", 9999);
        unsafe {
            let (len, s) = call(9999, None, 128);
            assert_eq!(s, expected);
            assert_eq!(len, expected.len() + 1);
        }
    }

    #[test]
    fn truncation_returns_full_length() {
        let expected = "failed to match"; // 15 chars
        unsafe {
            // bufsize 5 -> 4 chars + NUL
            let (len, s) = call(REG_NOMATCH, None, 5);
            assert_eq!(s, &expected[..4]);
            // returned length is the full untruncated length + NUL
            assert_eq!(len, expected.len() + 1);
        }
    }

    #[test]
    fn zero_buffer_size_just_returns_length() {
        unsafe {
            // errbuf_size==0: nothing written, only length returned
            let mut buf = [0 as c_char; 1];
            let len = pg_regerror(REG_NOMATCH, core::ptr::null(), buf.as_mut_ptr(), 0);
            assert_eq!(len, "failed to match".len() + 1);
        }
    }

    #[test]
    fn atoi_name_to_number() {
        unsafe {
            let (_len, s) = call(REG_ATOI, Some("REG_EBRACK"), 64);
            assert_eq!(s, "7");
        }
    }

    #[test]
    fn atoi_unknown_name_is_minus_one() {
        unsafe {
            let (_len, s) = call(REG_ATOI, Some("REG_NOPE"), 64);
            assert_eq!(s, "-1");
        }
    }

    #[test]
    fn itoa_number_to_name() {
        unsafe {
            let (_len, s) = call(REG_ITOA, Some("11"), 64);
            assert_eq!(s, "REG_ERANGE");
        }
    }

    #[test]
    fn itoa_unknown_number_formats_reg_n() {
        unsafe {
            let (_len, s) = call(REG_ITOA, Some("999"), 64);
            assert_eq!(s, "REG_999");
        }
    }

    #[test]
    fn okay_explain() {
        unsafe {
            let (_len, s) = call(REG_OKAY, None, 64);
            assert_eq!(s, "no errors detected");
        }
    }
}
