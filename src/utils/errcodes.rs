//! Translated from PostgreSQL src/include/utils/errcodes.h

// TODO(generated): produced by build.rs from ref/postgres/src/backend/utils/errcodes.txt.
// The full ERRCODE_* table is data-driven; only the SQLSTATE encoding helper
// (MAKE_SQLSTATE / PGSIXBIT, originally in utils/elog.h) lives here so generated
// constants can be const-evaluated.

/// PGSIXBIT: map a SQLSTATE character to its 6-bit code.
pub const fn pg_six_bit(ch: u8) -> u32 {
    ((ch.wrapping_sub(b'0')) & 0x3F) as u32
}

/// MAKE_SQLSTATE: pack five SQLSTATE characters into an int.
pub const fn make_sqlstate(ch1: u8, ch2: u8, ch3: u8, ch4: u8, ch5: u8) -> i32 {
    (pg_six_bit(ch1)
        + (pg_six_bit(ch2) << 6)
        + (pg_six_bit(ch3) << 12)
        + (pg_six_bit(ch4) << 18)
        + (pg_six_bit(ch5) << 24)) as i32
}
