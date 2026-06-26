//! Translated from PostgreSQL src/include/utils/varbit.h
//!
//! SQL BIT() / BIT VARYING(). VarBit is a toastable varlena (on-disk): a varlena
//! header, a bit count, then the bit string as a FAM.

use crate::c::bits8;
use crate::c::PG_INT32_MAX;
use crate::postgres::Datum;

pub const BITS_PER_BYTE: i32 = 8;

/// On-disk bit string. Modeled on varlena but element type is bits8. The unused
/// low-order bits of the last byte MUST be zeroes.
#[repr(C)]
pub struct VarBit {
    pub vl_len_: i32,  // varlena header (do not touch directly)
    pub bit_len: i32,  // number of valid bits
    // bits8 bit_dat[FLEXIBLE_ARRAY_MEMBER] follows (most significant byte first).
}
const _: () = assert!(core::mem::size_of::<VarBit>() == 8);

// fmgr interface
#[inline]
pub fn DatumGetVarBitP(x: Datum) -> *mut VarBit {
    unimplemented!() // PG_DETOAST_DATUM; TODO(ptr)
}
#[inline]
pub fn DatumGetVarBitPCopy(x: Datum) -> *mut VarBit {
    unimplemented!() // PG_DETOAST_DATUM_COPY; TODO(ptr)
}
#[inline]
pub fn VarBitPGetDatum(x: &VarBit) -> Datum {
    Datum(std::ptr::from_ref::<VarBit>(x) as usize)
}

/// Header overhead in addition to VARHDRSZ.
pub const VARBITHDRSZ: usize = core::mem::size_of::<i32>();

/// Maximum number of bits (leaves headroom so bitlen + X cannot overflow).
pub const VARBITMAXLEN: i32 = PG_INT32_MAX - BITS_PER_BYTE + 1;

/// Mask covering exactly one byte.
pub const BITMASK: u8 = 0xFF;

/// Number of bits in this bit string.
#[inline]
pub fn VARBITLEN(p: &VarBit) -> i32 {
    p.bit_len
}

/// Number of bytes needed to store a bit string of a given length.
#[inline]
pub const fn VARBITTOTALLEN(bitlen: i32) -> i32 {
    (bitlen + BITS_PER_BYTE - 1) / BITS_PER_BYTE + crate::c::VARHDRSZ + VARBITHDRSZ as i32
}

// VARBITS / VARBITBYTES / VARBITPAD / VARBITEND operate on the FAM + VARSIZE and
// need a safe slice accessor over the buffer (depends on detoast); deferred.
