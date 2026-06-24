//! Translated from PostgreSQL src/include/regex/regexport.h
//
// Accessors to export a compiled regex's NFA state graph and color char sets.
// In-memory accessor types. C `regex_t` is `pg_regex_t` in this port.

use crate::mb::pg_wchar::pg_wchar;
use crate::regex::regex::pg_regex_t;

// These must match corresponding macros in regguts.h.
pub const COLOR_WHITE: i32 = 0;    // color for chars not appearing in regex
pub const COLOR_RAINBOW: i32 = -2; // all colors except pseudocolors

/// Information about one arc of a regex's NFA.
#[derive(Debug, Clone, Copy)]
pub struct regex_arc_t {
    pub co: i32, // label (character-set color) of arc
    pub to: i32, // next state number
}

// Functions for gathering information about NFA states and arcs.
pub fn pg_reg_getnumstates(_regex: &pg_regex_t) -> i32 {
    unimplemented!()
}

pub fn pg_reg_getinitialstate(_regex: &pg_regex_t) -> i32 {
    unimplemented!()
}

pub fn pg_reg_getfinalstate(_regex: &pg_regex_t) -> i32 {
    unimplemented!()
}

pub fn pg_reg_getnumoutarcs(_regex: &pg_regex_t, _st: i32) -> i32 {
    unimplemented!()
}

/// Fills `arcs` (up to its length) with the out-arcs of state `st`.
pub fn pg_reg_getoutarcs(_regex: &pg_regex_t, _st: i32, _arcs: &mut [regex_arc_t]) {
    unimplemented!()
}

// Functions for gathering information about colors.
pub fn pg_reg_getnumcolors(_regex: &pg_regex_t) -> i32 {
    unimplemented!()
}

/// C returns int (boolean); true if color marks begin-of-line/string.
pub fn pg_reg_colorisbegin(_regex: &pg_regex_t, _co: i32) -> bool {
    unimplemented!()
}

pub fn pg_reg_colorisend(_regex: &pg_regex_t, _co: i32) -> bool {
    unimplemented!()
}

pub fn pg_reg_getnumcharacters(_regex: &pg_regex_t, _co: i32) -> i32 {
    unimplemented!()
}

/// Fills `chars` (up to its length) with the characters of color `co`.
pub fn pg_reg_getcharacters(_regex: &pg_regex_t, _co: i32, _chars: &mut [pg_wchar]) {
    unimplemented!()
}
