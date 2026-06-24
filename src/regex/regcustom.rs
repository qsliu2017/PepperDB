//! Translated from PostgreSQL src/include/regex/regcustom.h
//
// Regex engine build-config header. The allocation macros (MALLOC/FREE/REALLOC ->
// palloc, assert -> Assert, INTERRUPT -> CHECK_FOR_INTERRUPTS) are C build-time
// substitutions with no Rust analogue and are omitted. The character-type and
// range constants below carry over.

use crate::mb::pg_wchar::pg_wchar;

/// the internal character type itself
pub type chr = pg_wchar;
/// unsigned type that will hold a `chr`
pub type uchr = u32;

/// turn a char literal into a chr literal
pub const fn CHR(c: u8) -> chr {
    c as chr
}

/// turn a chr digit into its value
pub const fn DIGITVAL(c: chr) -> chr {
    c - b'0' as chr
}

/// bits in a chr
pub const CHRBITS: i32 = 32;
/// smallest chr value
pub const CHR_MIN: chr = 0x00000000;
/// largest chr value (CHR_MAX-CHR_MIN+1 must fit in an int)
pub const CHR_MAX: chr = 0x7ffffffe;

/// Check if a chr value is in range (chr is unsigned, so only the upper bound).
pub const fn CHR_IS_IN_RANGE(c: chr) -> bool {
    c <= CHR_MAX
}

/// Cutoff between "simple" and "complicated" color-map processing.
pub const MAX_SIMPLE_CHR: chr = 0x7FF;

// Functions operating on chr: C macros aliasing pg_wc_isalnum/isalpha/isdigit/
// isspace (mb/pg_wchar.h, not yet translated). Stubbed signatures; bodies arrive
// when pg_wc_* land. TODO(import): forward to crate::mb::pg_wchar::pg_wc_* once
// those exist.
pub fn iscalnum(_x: chr) -> bool {
    unimplemented!()
}
pub fn iscalpha(_x: chr) -> bool {
    unimplemented!()
}
pub fn iscdigit(_x: chr) -> bool {
    unimplemented!()
}
pub fn iscspace(_x: chr) -> bool {
    unimplemented!()
}
