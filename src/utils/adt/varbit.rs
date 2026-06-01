//! Translation of postgres/src/backend/utils/adt/varbit.c (+ varbit.h)
//!
//! Functions for the SQL datatypes BIT() and BIT VARYING().
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: common/int.h -> crate::common::int (pg_add_s32_overflow),
//! libpq/pqformat.h -> crate::libpq::pqformat (pq_getmsgint/pq_copymsgbytes/
//! pq_begintypsend/pq_sendint32/pq_sendbytes/pq_endtypsend), port/pg_bitutils.h ->
//! crate::port::pg_bitutils (pg_popcount), utils/varbit.h merged in below.  The VAR*
//! macros come from crate::varatt.  libc memcpy/memcmp bound via extern "C".
//!
//! TRANSLATED: bit_in/bit_out/bit_recv/bit_send, varbit_in/varbit_out/varbit_recv/
//! varbit_send, bit()/varbit() length casts, bittypmodin/out + varbittypmodin/out
//! (minus the ArrayType decode, see below), biteq/bitne/bitlt/bitle/bitgt/bitge/
//! bitcmp (+ internal bit_cmp), bit_and/bit_or/bitxor/bitnot/bitshiftleft/
//! bitshiftright, bitcat (+ bit_catenate), bitsubstr/bitsubstr_no_len (+ bitsubstring),
//! bitoverlay/bitoverlay_no_len (+ bit_overlay), bit_bit_count, bitlength,
//! bitoctetlength, bitposition, bitsetbit, bitgetbit, bitfromint4/bittoint4,
//! bitfromint8/bittoint8.
//!
//! STUBBED (deps not yet ported):
//!  - anybit_typmodin / bittypmodin / varbittypmodin: utils/array.h ArrayType +
//!    ArrayGetIntegerTypmods (PG_GETARG_ARRAYTYPE_P) not yet translated.
//!  - varbit_support: nodes/supportnodes.h (SupportRequestSimplify) + nodes/nodeFuncs.h
//!    (exprTypmod / relabel_to_typmod) not yet translated.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;
// GLOB-AMBIGUITY: both crate::varatt and crate::utils::fmgr export
// pg_detoast_datum_packed; the explicit import here wins over the two globs.
use crate::varatt::pg_detoast_datum_packed;
use crate::{
    PG_GETARG_BOOL, PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_POINTER,
    PG_RETURN_BOOL, PG_RETURN_CSTRING, PG_RETURN_INT32, PG_RETURN_INT64,
};
use crate::c::{bits8, int32, int64, uint32, uint64};
use crate::common::int::pg_add_s32_overflow;
use crate::libpq::pqformat::{
    pq_begintypsend, pq_copymsgbytes, pq_endtypsend, pq_getmsgint, pq_sendbytes, pq_sendint32,
};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::nodes::nodes::Node;
use crate::port::pg_bitutils::pg_popcount;
use crate::postgres::{DatumGetPointer, Int32GetDatum, PointerGetDatum};
use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_STRING_DATA_LENGTH_MISMATCH: c_int = 0;
const ERRCODE_STRING_DATA_RIGHT_TRUNCATION: c_int = 0;
const ERRCODE_INVALID_BINARY_REPRESENTATION: c_int = 0;
const ERRCODE_SUBSTRING_ERROR: c_int = 0;
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;
const ERRCODE_ARRAY_SUBSCRIPT_ERROR: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

// ----------------------------------------------------------------
//   utils/varbit.h merged in
// ----------------------------------------------------------------

/*
 * Modeled on struct varlena from postgres.h, but data type is bits8.
 *
 * Caution: if bit_len is not a multiple of BITS_PER_BYTE, the low-order
 * bits of the last byte of bit_dat[] are unused and MUST be zeroes.
 * (This allows bit_cmp() to not bother masking the last byte.)
 * Also, there should not be any excess bytes counted in the header length.
 */
#[repr(C)]
pub struct VarBit {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub bit_len: int32, /* number of valid bits */
    pub bit_dat: [bits8; FLEXIBLE_ARRAY_MEMBER], /* bit string, most sig. byte first */
}

/*
 * fmgr interface helpers (varbit.h).  BIT and BIT VARYING are toastable varlena
 * types with the same representation, so one set of helpers serves both.
 *
 * # Safety
 * `X` is a Datum holding a (possibly toasted) VarBit pointer.
 */
#[inline]
pub unsafe fn DatumGetVarBitP(X: Datum) -> *mut VarBit {
    pg_detoast_datum_packed(DatumGetPointer(X) as *mut c_void) as *mut VarBit
}
#[inline]
pub unsafe fn VarBitPGetDatum(X: *const VarBit) -> Datum {
    PointerGetDatum(X as *const c_void)
}
// PG_GETARG_VARBIT_P(n)   == DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, n))
// PG_RETURN_VARBIT_P(x)   == return VarBitPGetDatum(x)

/* Header overhead *in addition to* VARHDRSZ */
pub const VARBITHDRSZ: usize = core::mem::size_of::<int32>();

/* Number of bits in this bit string */
#[inline]
pub unsafe fn VARBITLEN(PTR: *const VarBit) -> c_int {
    (*PTR).bit_len
}
/* Assign to ((VarBit *) PTR)->bit_len (the C macro is used as an lvalue) */
#[inline]
pub unsafe fn set_VARBITLEN(PTR: *mut VarBit, v: c_int) {
    (*PTR).bit_len = v;
}
/* Pointer to the first byte containing bit string data */
#[inline]
pub unsafe fn VARBITS(PTR: *mut VarBit) -> *mut bits8 {
    (*PTR).bit_dat.as_mut_ptr()
}
/* Number of bytes in the data section of a bit string */
#[inline]
pub unsafe fn VARBITBYTES(PTR: *const VarBit) -> c_int {
    VARSIZE(PTR as *const c_char) as c_int - VARHDRSZ - VARBITHDRSZ as c_int
}
/* Padding of the bit string at the end (in bits) */
#[inline]
pub unsafe fn VARBITPAD(PTR: *const VarBit) -> c_int {
    VARBITBYTES(PTR) * BITS_PER_BYTE - VARBITLEN(PTR)
}
/* Number of bytes needed to store a bit string of a given length */
#[inline]
pub fn VARBITTOTALLEN(BITLEN: c_int) -> c_int {
    (BITLEN + BITS_PER_BYTE - 1) / BITS_PER_BYTE + VARHDRSZ + VARBITHDRSZ as c_int
}
/*
 * Maximum number of bits.  Several code sites assume no overflow from
 * computing bitlen + X; VARBITTOTALLEN() has the largest such X.
 */
pub const VARBITMAXLEN: c_int = i32::MAX - BITS_PER_BYTE + 1;
/* pointer beyond the end of the bit string (like end() in STL containers) */
#[inline]
pub unsafe fn VARBITEND(PTR: *mut VarBit) -> *mut bits8 {
    (PTR as *mut bits8).add(VARSIZE(PTR as *const c_char) as usize)
}
/* Mask that will cover exactly one byte, i.e. BITS_PER_BYTE bits */
pub const BITMASK: bits8 = 0xFF;

// BITS_PER_BYTE (c.h); VARHDRSZ comes from crate::c (prelude).
const BITS_PER_BYTE: c_int = 8;

// MaxAttrSize (access/htup_details.h): largest attribute (10 MB).
const MaxAttrSize: c_int = 10 * 1024 * 1024;

// HEXDIG(z): used only by the (disabled-by-#if 0) hex branch of bit_out below.
#[allow(dead_code)]
#[inline]
fn HEXDIG(z: c_int) -> c_char {
    (if z < 10 { z + b'0' as c_int } else { z - 10 + b'A' as c_int }) as c_char
}

/*
 * VARBIT_PAD(vb) - mask off any bits that should be zero in the last byte.
 *
 * # Safety
 * `vb` is a valid, correctly-sized VarBit.
 */
#[inline]
unsafe fn VARBIT_PAD(vb: *mut VarBit) {
    let pad_: int32 = VARBITPAD(vb);
    Assert!(pad_ >= 0 && pad_ < BITS_PER_BYTE);
    if pad_ > 0 {
        *(VARBITS(vb).add(VARBITBYTES(vb) as usize - 1)) &= BITMASK << pad_;
    }
}

/*
 * VARBIT_PAD_LAST(vb, ptr) - like VARBIT_PAD but the caller already has a pointer
 * to the last-plus-one byte, which saves a cycle or two.
 *
 * # Safety
 * `vb` is a valid VarBit; `ptr` is one past the last data byte of `vb`.
 */
#[inline]
unsafe fn VARBIT_PAD_LAST(vb: *mut VarBit, ptr: *mut bits8) {
    let pad_: int32 = VARBITPAD(vb);
    Assert!(pad_ >= 0 && pad_ < BITS_PER_BYTE);
    if pad_ > 0 {
        *(ptr.sub(1)) &= BITMASK << pad_;
    }
}

/* VARBIT_CORRECTLY_PADDED(vb): assertion helper, ((void) 0) without assertions. */
#[inline]
unsafe fn VARBIT_CORRECTLY_PADDED(_vb: *mut VarBit) {
    // Under USE_ASSERT_CHECKING this asserts the pad bits are zero; we keep it a no-op.
}

// ----------------------------------------------------------------
//   varbit.c
// ----------------------------------------------------------------

/*
 * common code for bittypmodin and varbittypmodin
 *
 * TODO(pg-port): utils/array.h ArrayType + ArrayGetIntegerTypmods not yet translated.
 *
 * # Safety
 * `ta` is a valid ArrayType*; `typename` a NUL-terminated C string.
 */
#[allow(dead_code)]
unsafe fn anybit_typmodin(ta: *mut c_void, typename: *const c_char) -> int32 {
    // C body:
    //   tl = ArrayGetIntegerTypmods(ta, &n);
    //   if (n != 1) ereport(... "invalid type modifier");
    //   if (*tl < 1) ereport(... "length for type %s must be at least 1");
    //   if (*tl > (MaxAttrSize * BITS_PER_BYTE))
    //       ereport(... "length for type %s cannot exceed %d", MaxAttrSize*BITS_PER_BYTE);
    //   typmod = *tl; return typmod;
    let _ = (ta, typename, MaxAttrSize, ERRCODE_INVALID_PARAMETER_VALUE);
    unimplemented!("anybit_typmodin: utils/array.h (ArrayGetIntegerTypmods) not yet translated")
}

/*
 * common code for bittypmodout and varbittypmodout
 *
 * # Safety
 * Returns a freshly palloc'd NUL-terminated C string.
 */
unsafe fn anybit_typmodout(typmod: int32) -> *mut c_char {
    let res: *mut c_char = palloc(64) as *mut c_char;

    if typmod >= 0 {
        // snprintf(res, 64, "(%d)", typmod);
        let s = format!("({})\0", typmod);
        let bytes = s.as_bytes();
        let n = core::cmp::min(bytes.len(), 64);
        core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, res, n);
    } else {
        *res = b'\0' as c_char;
    }

    res
}

/*
 * bit_in -
 *	  converts a char string to the internal representation of a bitstring.
 *		  The length is determined by the number of bits required plus
 *		  VARHDRSZ bytes or from atttypmod.
 */
pub unsafe fn bit_in(fcinfo: FunctionCallInfo) -> Datum {
    let input_string: *const c_char = PG_GETARG_DATUM!(fcinfo, 0) as *const c_char; // PG_GETARG_CSTRING
    let mut atttypmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let escontext: *mut Node = (*fcinfo).context;
    let result: *mut VarBit; /* The resulting bit string */
    let mut sp: *const c_char; /* pointer into the character string */
    let mut r: *mut bits8; /* pointer into the result */
    let len: c_int; /* Length of the whole data structure */
    let bitlen: c_int; /* Number of bits in the bit string */
    let slen: c_int; /* Length of the input string */
    let bit_not_hex: bool; /* false = hex string  true = bit string */
    let mut bc: c_int;
    let mut x: bits8 = 0;
    let _ = escontext; // TODO(pg-port): ErrorSaveContext soft errors (ereturn -> hard ERROR)

    /* Check that the first character is a b or an x */
    if *input_string == b'b' as c_char || *input_string == b'B' as c_char {
        bit_not_hex = true;
        sp = input_string.add(1);
    } else if *input_string == b'x' as c_char || *input_string == b'X' as c_char {
        bit_not_hex = false;
        sp = input_string.add(1);
    } else {
        /*
         * Otherwise it's binary.  This allows things like cast('1001' as bit)
         * to work transparently.
         */
        bit_not_hex = true;
        sp = input_string;
    }

    /*
     * Determine bitlength from input string.  MaxAllocSize ensures a regular
     * input is small enough, but we must check hex input.
     */
    slen = strlen(sp) as c_int;
    if bit_not_hex {
        bitlen = slen;
    } else {
        if slen > VARBITMAXLEN / 4 {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!(
                    "bit string length exceeds the maximum allowed ({})",
                    VARBITMAXLEN
                )
            );
        }
        bitlen = slen * 4;
    }

    /*
     * Sometimes atttypmod is not supplied. If it is supplied we need to make
     * sure that the bitstring fits.
     */
    if atttypmod <= 0 {
        atttypmod = bitlen;
    } else if bitlen != atttypmod {
        let _ = errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH);
        ereport!(
            ERROR,
            errmsg!(
                "bit string length {} does not match type bit({})",
                bitlen,
                atttypmod
            )
        );
    }

    len = VARBITTOTALLEN(atttypmod);
    /* set to 0 so that *r is always initialised and string is zero-padded */
    result = palloc0(len as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, len);
    set_VARBITLEN(result, atttypmod);

    r = VARBITS(result);
    if bit_not_hex {
        /* Parse the bit representation of the string */
        /* We know it fits, as bitlen was compared to atttypmod */
        x = HIGHBIT;
        while *sp != 0 {
            if *sp == b'1' as c_char {
                *r |= x;
            } else if *sp != b'0' as c_char {
                let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
                ereport!(
                    ERROR,
                    errmsg!("\"{}\" is not a valid binary digit", mblen_str(sp))
                );
            }

            x >>= 1;
            if x == 0 {
                x = HIGHBIT;
                r = r.add(1);
            }
            sp = sp.add(1);
        }
    } else {
        /* Parse the hex representation of the string */
        bc = 0;
        while *sp != 0 {
            let c = *sp as u8;
            if c >= b'0' && c <= b'9' {
                x = c - b'0';
            } else if c >= b'A' && c <= b'F' {
                x = (c - b'A') + 10;
            } else if c >= b'a' && c <= b'f' {
                x = (c - b'a') + 10;
            } else {
                let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
                ereport!(
                    ERROR,
                    errmsg!("\"{}\" is not a valid hexadecimal digit", mblen_str(sp))
                );
            }

            if bc != 0 {
                *r |= x;
                r = r.add(1);
                bc = 0;
            } else {
                *r = x << 4;
                bc = 1;
            }
            sp = sp.add(1);
        }
    }

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

pub unsafe fn bit_out(fcinfo: FunctionCallInfo) -> Datum {
    // #if 1: same as varbit output
    varbit_out(fcinfo)
}

/*
 *		bit_recv			- converts external binary format to bit
 */
pub unsafe fn bit_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let atttypmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut VarBit;
    let len: c_int;
    let bitlen: c_int;

    bitlen = pq_getmsgint(buf, core::mem::size_of::<int32>() as c_int) as c_int;
    if bitlen < 0 || bitlen > VARBITMAXLEN {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        ereport!(ERROR, errmsg!("invalid length in external bit string"));
    }

    /*
     * Sometimes atttypmod is not supplied. If it is supplied we need to make
     * sure that the bitstring fits.
     */
    if atttypmod > 0 && bitlen != atttypmod {
        let _ = errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH);
        ereport!(
            ERROR,
            errmsg!(
                "bit string length {} does not match type bit({})",
                bitlen,
                atttypmod
            )
        );
    }

    len = VARBITTOTALLEN(bitlen);
    result = palloc(len as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, len);
    set_VARBITLEN(result, bitlen);

    pq_copymsgbytes(buf, VARBITS(result) as *mut c_void, VARBITBYTES(result));

    /* Make sure last byte is correctly zero-padded */
    VARBIT_PAD(result);

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 *		bit_send			- converts bit to binary format
 */
pub unsafe fn bit_send(fcinfo: FunctionCallInfo) -> Datum {
    /* Exactly the same as varbit_send, so share code */
    varbit_send(fcinfo)
}

/*
 * bit()
 * Converts a bit() type to a specific internal length.
 * len is the bitlength specified in the column definition.
 *
 * If doing implicit cast, raise error when source data is wrong length.
 * If doing explicit cast, silently truncate or zero-pad to specified length.
 */
pub unsafe fn bit(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let len: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let isExplicit: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let result: *mut VarBit;
    let rlen: c_int;

    /* No work if typmod is invalid or supplied data matches it already */
    if len <= 0 || len > VARBITMAXLEN || len == VARBITLEN(arg) {
        return VarBitPGetDatum(arg); // PG_RETURN_VARBIT_P
    }

    if !isExplicit {
        let _ = errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH);
        ereport!(
            ERROR,
            errmsg!(
                "bit string length {} does not match type bit({})",
                VARBITLEN(arg),
                len
            )
        );
    }

    rlen = VARBITTOTALLEN(len);
    /* set to 0 so that string is zero-padded */
    result = palloc0(rlen as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, rlen);
    set_VARBITLEN(result, len);

    memcpy(
        VARBITS(result) as *mut c_void,
        VARBITS(arg) as *const c_void,
        Min(VARBITBYTES(result), VARBITBYTES(arg)) as usize,
    );

    /*
     * Make sure last byte is zero-padded if needed.  This is useless but safe
     * if source data was shorter than target length (we assume the last byte
     * of the source data was itself correctly zero-padded).
     */
    VARBIT_PAD(result);

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

pub unsafe fn bittypmodin(fcinfo: FunctionCallInfo) -> Datum {
    // C: ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);
    //    PG_RETURN_INT32(anybit_typmodin(ta, "bit"));
    // TODO(pg-port): utils/array.h (PG_GETARG_ARRAYTYPE_P / ArrayGetIntegerTypmods).
    let _ = fcinfo;
    unimplemented!("bittypmodin: utils/array.h (ArrayType) not yet translated")
}

pub unsafe fn bittypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_CSTRING!(anybit_typmodout(typmod));
}

/*
 * varbit_in -
 *	  converts a string to the internal representation of a bitstring.
 *		This is the same as bit_in except that atttypmod is taken as
 *		the maximum length, not the exact length to force the bitstring to.
 */
pub unsafe fn varbit_in(fcinfo: FunctionCallInfo) -> Datum {
    let input_string: *const c_char = PG_GETARG_DATUM!(fcinfo, 0) as *const c_char; // PG_GETARG_CSTRING
    let mut atttypmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let escontext: *mut Node = (*fcinfo).context;
    let result: *mut VarBit; /* The resulting bit string */
    let mut sp: *const c_char; /* pointer into the character string */
    let mut r: *mut bits8; /* pointer into the result */
    let len: c_int; /* Length of the whole data structure */
    let bitlen: c_int; /* Number of bits in the bit string */
    let slen: c_int; /* Length of the input string */
    let bit_not_hex: bool; /* false = hex string  true = bit string */
    let mut bc: c_int;
    let mut x: bits8 = 0;
    let _ = escontext; // TODO(pg-port): ErrorSaveContext soft errors (ereturn -> hard ERROR)

    /* Check that the first character is a b or an x */
    if *input_string == b'b' as c_char || *input_string == b'B' as c_char {
        bit_not_hex = true;
        sp = input_string.add(1);
    } else if *input_string == b'x' as c_char || *input_string == b'X' as c_char {
        bit_not_hex = false;
        sp = input_string.add(1);
    } else {
        bit_not_hex = true;
        sp = input_string;
    }

    /*
     * Determine bitlength from input string.  MaxAllocSize ensures a regular
     * input is small enough, but we must check hex input.
     */
    slen = strlen(sp) as c_int;
    if bit_not_hex {
        bitlen = slen;
    } else {
        if slen > VARBITMAXLEN / 4 {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!(
                    "bit string length exceeds the maximum allowed ({})",
                    VARBITMAXLEN
                )
            );
        }
        bitlen = slen * 4;
    }

    /*
     * Sometimes atttypmod is not supplied. If it is supplied we need to make
     * sure that the bitstring fits.
     */
    if atttypmod <= 0 {
        atttypmod = bitlen;
    } else if bitlen > atttypmod {
        let _ = errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
        ereport!(
            ERROR,
            errmsg!("bit string too long for type bit varying({})", atttypmod)
        );
    }

    len = VARBITTOTALLEN(bitlen);
    /* set to 0 so that *r is always initialised and string is zero-padded */
    result = palloc0(len as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, len);
    set_VARBITLEN(result, Min(bitlen, atttypmod));

    r = VARBITS(result);
    if bit_not_hex {
        /* Parse the bit representation of the string */
        /* We know it fits, as bitlen was compared to atttypmod */
        x = HIGHBIT;
        while *sp != 0 {
            if *sp == b'1' as c_char {
                *r |= x;
            } else if *sp != b'0' as c_char {
                let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
                ereport!(
                    ERROR,
                    errmsg!("\"{}\" is not a valid binary digit", mblen_str(sp))
                );
            }

            x >>= 1;
            if x == 0 {
                x = HIGHBIT;
                r = r.add(1);
            }
            sp = sp.add(1);
        }
    } else {
        /* Parse the hex representation of the string */
        bc = 0;
        while *sp != 0 {
            let c = *sp as u8;
            if c >= b'0' && c <= b'9' {
                x = c - b'0';
            } else if c >= b'A' && c <= b'F' {
                x = (c - b'A') + 10;
            } else if c >= b'a' && c <= b'f' {
                x = (c - b'a') + 10;
            } else {
                let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
                ereport!(
                    ERROR,
                    errmsg!("\"{}\" is not a valid hexadecimal digit", mblen_str(sp))
                );
            }

            if bc != 0 {
                *r |= x;
                r = r.add(1);
                bc = 0;
            } else {
                *r = x << 4;
                bc = 1;
            }
            sp = sp.add(1);
        }
    }

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 * varbit_out -
 *	  Prints the string as bits to preserve length accurately
 *
 * XXX varbit_recv() and hex input to varbit_in() can load a value that this
 * cannot emit.  Consider using hex output for such values.
 */
pub unsafe fn varbit_out(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut c_char;
    let mut r: *mut c_char;
    let mut sp: *mut bits8;
    let mut x: bits8;
    let mut i: c_int;
    let mut k: c_int;
    let len: c_int;

    /* Assertion to help catch any bit functions that don't pad correctly */
    VARBIT_CORRECTLY_PADDED(s);

    len = VARBITLEN(s);
    result = palloc((len + 1) as Size) as *mut c_char;
    sp = VARBITS(s);
    r = result;
    i = 0;
    while i <= len - BITS_PER_BYTE {
        /* print full bytes */
        x = *sp;
        k = 0;
        while k < BITS_PER_BYTE {
            *r = if IS_HIGHBIT_SET(x) { b'1' as c_char } else { b'0' as c_char };
            r = r.add(1);
            x <<= 1;
            k += 1;
        }
        i += BITS_PER_BYTE;
        sp = sp.add(1);
    }
    if i < len {
        /* print the last partial byte */
        x = *sp;
        k = i;
        while k < len {
            *r = if IS_HIGHBIT_SET(x) { b'1' as c_char } else { b'0' as c_char };
            r = r.add(1);
            x <<= 1;
            k += 1;
        }
    }
    *r = b'\0' as c_char;

    PG_RETURN_CSTRING!(result);
}

/*
 *		varbit_recv			- converts external binary format to varbit
 *
 * External format is the bitlen as an int32, then the byte array.
 */
pub unsafe fn varbit_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let atttypmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut VarBit;
    let len: c_int;
    let bitlen: c_int;

    bitlen = pq_getmsgint(buf, core::mem::size_of::<int32>() as c_int) as c_int;
    if bitlen < 0 || bitlen > VARBITMAXLEN {
        let _ = errcode(ERRCODE_INVALID_BINARY_REPRESENTATION);
        ereport!(ERROR, errmsg!("invalid length in external bit string"));
    }

    /*
     * Sometimes atttypmod is not supplied. If it is supplied we need to make
     * sure that the bitstring fits.
     */
    if atttypmod > 0 && bitlen > atttypmod {
        let _ = errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
        ereport!(
            ERROR,
            errmsg!("bit string too long for type bit varying({})", atttypmod)
        );
    }

    len = VARBITTOTALLEN(bitlen);
    result = palloc(len as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, len);
    set_VARBITLEN(result, bitlen);

    pq_copymsgbytes(buf, VARBITS(result) as *mut c_void, VARBITBYTES(result));

    /* Make sure last byte is correctly zero-padded */
    VARBIT_PAD(result);

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 *		varbit_send			- converts varbit to binary format
 */
pub unsafe fn varbit_send(fcinfo: FunctionCallInfo) -> Datum {
    let s: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint32(&mut buf, VARBITLEN(s) as uint32);
    pq_sendbytes(&mut buf, VARBITS(s) as *const c_void, VARBITBYTES(s));
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void); // PG_RETURN_BYTEA_P
}

/*
 * varbit_support()
 *
 * Planner support function for the varbit() length coercion function.
 */
pub unsafe fn varbit_support(fcinfo: FunctionCallInfo) -> Datum {
    // C body uses IsA(rawreq, SupportRequestSimplify), FuncExpr, exprTypmod,
    // relabel_to_typmod.
    // TODO(pg-port): nodes/supportnodes.h + nodes/nodeFuncs.h not yet translated.
    let _ = fcinfo;
    unimplemented!("varbit_support: nodes/supportnodes.h (SupportRequestSimplify) not yet translated")
}

/*
 * varbit()
 * Converts a varbit() type to a specific internal length.
 * len is the maximum bitlength specified in the column definition.
 *
 * If doing implicit cast, raise error when source data is too long.
 * If doing explicit cast, silently truncate to max length.
 */
pub unsafe fn varbit(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let len: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let isExplicit: bool = PG_GETARG_BOOL!(fcinfo, 2);
    let result: *mut VarBit;
    let rlen: c_int;

    /* No work if typmod is invalid or supplied data matches it already */
    if len <= 0 || len >= VARBITLEN(arg) {
        return VarBitPGetDatum(arg); // PG_RETURN_VARBIT_P
    }

    if !isExplicit {
        let _ = errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
        ereport!(
            ERROR,
            errmsg!("bit string too long for type bit varying({})", len)
        );
    }

    rlen = VARBITTOTALLEN(len);
    result = palloc(rlen as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, rlen);
    set_VARBITLEN(result, len);

    memcpy(
        VARBITS(result) as *mut c_void,
        VARBITS(arg) as *const c_void,
        VARBITBYTES(result) as usize,
    );

    /* Make sure last byte is correctly zero-padded */
    VARBIT_PAD(result);

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

pub unsafe fn varbittypmodin(fcinfo: FunctionCallInfo) -> Datum {
    // C: ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);
    //    PG_RETURN_INT32(anybit_typmodin(ta, "varbit"));
    // TODO(pg-port): utils/array.h (PG_GETARG_ARRAYTYPE_P / ArrayGetIntegerTypmods).
    let _ = fcinfo;
    unimplemented!("varbittypmodin: utils/array.h (ArrayType) not yet translated")
}

pub unsafe fn varbittypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_CSTRING!(anybit_typmodout(typmod));
}

/*
 * Comparison operators
 *
 * We only need one set of comparison operators for bitstrings, as the lengths
 * are stored in the same way for zero-padded and varying bit strings.
 */

/*
 * bit_cmp
 *
 * Compares two bitstrings and returns <0, 0, >0 depending on whether the first
 * string is smaller, equal, or bigger than the second. All bits are considered
 * and additional zero bits may make one string smaller/larger than the other,
 * even if their zero-padded values would be the same.
 */
unsafe fn bit_cmp(arg1: *mut VarBit, arg2: *mut VarBit) -> int32 {
    let bitlen1: c_int;
    let bitlen2: c_int;
    let bytelen1: c_int;
    let bytelen2: c_int;
    let mut cmp: int32;

    bytelen1 = VARBITBYTES(arg1);
    bytelen2 = VARBITBYTES(arg2);

    cmp = memcmp(
        VARBITS(arg1) as *const c_void,
        VARBITS(arg2) as *const c_void,
        Min(bytelen1, bytelen2) as usize,
    );
    if cmp == 0 {
        bitlen1 = VARBITLEN(arg1);
        bitlen2 = VARBITLEN(arg2);
        if bitlen1 != bitlen2 {
            cmp = if bitlen1 < bitlen2 { -1 } else { 1 };
        }
    }
    cmp
}

pub unsafe fn biteq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: bool;
    let bitlen1: c_int;
    let bitlen2: c_int;

    bitlen1 = VARBITLEN(arg1);
    bitlen2 = VARBITLEN(arg2);

    /* fast path for different-length inputs */
    if bitlen1 != bitlen2 {
        result = false;
    } else {
        result = bit_cmp(arg1, arg2) == 0;
    }

    /* PG_FREE_IF_COPY(arg1, 0); PG_FREE_IF_COPY(arg2, 1): no-op, detoast of an in-line datum is identity. */

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bitne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: bool;
    let bitlen1: c_int;
    let bitlen2: c_int;

    bitlen1 = VARBITLEN(arg1);
    bitlen2 = VARBITLEN(arg2);

    /* fast path for different-length inputs */
    if bitlen1 != bitlen2 {
        result = true;
    } else {
        result = bit_cmp(arg1, arg2) != 0;
    }

    /* PG_FREE_IF_COPY(arg1, 0); PG_FREE_IF_COPY(arg2, 1): no-op, detoast of an in-line datum is identity. */

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bitlt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: bool;

    result = bit_cmp(arg1, arg2) < 0;

    /* PG_FREE_IF_COPY(arg1, 0); PG_FREE_IF_COPY(arg2, 1): no-op, detoast of an in-line datum is identity. */

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bitle(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: bool;

    result = bit_cmp(arg1, arg2) <= 0;

    /* PG_FREE_IF_COPY(arg1, 0); PG_FREE_IF_COPY(arg2, 1): no-op, detoast of an in-line datum is identity. */

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bitgt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: bool;

    result = bit_cmp(arg1, arg2) > 0;

    /* PG_FREE_IF_COPY(arg1, 0); PG_FREE_IF_COPY(arg2, 1): no-op, detoast of an in-line datum is identity. */

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bitge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: bool;

    result = bit_cmp(arg1, arg2) >= 0;

    /* PG_FREE_IF_COPY(arg1, 0); PG_FREE_IF_COPY(arg2, 1): no-op, detoast of an in-line datum is identity. */

    PG_RETURN_BOOL!(result);
}

pub unsafe fn bitcmp(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: int32;

    result = bit_cmp(arg1, arg2);

    /* PG_FREE_IF_COPY(arg1, 0); PG_FREE_IF_COPY(arg2, 1): no-op, detoast of an in-line datum is identity. */

    PG_RETURN_INT32!(result);
}

/*
 * bitcat
 * Concatenation of bit strings
 */
pub unsafe fn bitcat(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));

    return VarBitPGetDatum(bit_catenate(arg1, arg2)); // PG_RETURN_VARBIT_P
}

unsafe fn bit_catenate(arg1: *mut VarBit, arg2: *mut VarBit) -> *mut VarBit {
    let result: *mut VarBit;
    let bitlen1: c_int;
    let bitlen2: c_int;
    let bytelen: c_int;
    let bit1pad: c_int;
    let bit2shift: c_int;
    let mut pr: *mut bits8;
    let mut pa: *mut bits8;

    bitlen1 = VARBITLEN(arg1);
    bitlen2 = VARBITLEN(arg2);

    if bitlen1 > VARBITMAXLEN - bitlen2 {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "bit string length exceeds the maximum allowed ({})",
                VARBITMAXLEN
            )
        );
    }
    bytelen = VARBITTOTALLEN(bitlen1 + bitlen2);

    result = palloc(bytelen as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, bytelen);
    set_VARBITLEN(result, bitlen1 + bitlen2);

    /* Copy the first bitstring in */
    memcpy(
        VARBITS(result) as *mut c_void,
        VARBITS(arg1) as *const c_void,
        VARBITBYTES(arg1) as usize,
    );

    /* Copy the second bit string */
    bit1pad = VARBITPAD(arg1);
    if bit1pad == 0 {
        memcpy(
            VARBITS(result).add(VARBITBYTES(arg1) as usize) as *mut c_void,
            VARBITS(arg2) as *const c_void,
            VARBITBYTES(arg2) as usize,
        );
    } else if bitlen2 > 0 {
        /* We need to shift all the bits to fit */
        bit2shift = BITS_PER_BYTE - bit1pad;
        pr = VARBITS(result).add(VARBITBYTES(arg1) as usize - 1);
        pa = VARBITS(arg2);
        while pa < VARBITEND(arg2) {
            *pr |= (*pa >> bit2shift) & BITMASK;
            pr = pr.add(1);
            if pr < VARBITEND(result) {
                *pr = (*pa << bit1pad) & BITMASK;
            }
            pa = pa.add(1);
        }
    }

    /* The pad bits should be already zero at this point */

    result
}

/*
 * bitsubstr
 * retrieve a substring from the bit string.
 * Note, s is 1-based.
 * SQL draft 6.10 9)
 */
pub unsafe fn bitsubstr(fcinfo: FunctionCallInfo) -> Datum {
    return VarBitPGetDatum(bitsubstring(
        DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0)),
        PG_GETARG_INT32!(fcinfo, 1),
        PG_GETARG_INT32!(fcinfo, 2),
        false,
    )); // PG_RETURN_VARBIT_P
}

pub unsafe fn bitsubstr_no_len(fcinfo: FunctionCallInfo) -> Datum {
    return VarBitPGetDatum(bitsubstring(
        DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0)),
        PG_GETARG_INT32!(fcinfo, 1),
        -1,
        true,
    )); // PG_RETURN_VARBIT_P
}

unsafe fn bitsubstring(
    arg: *mut VarBit,
    s: int32,
    l: int32,
    length_not_specified: bool,
) -> *mut VarBit {
    let result: *mut VarBit;
    let bitlen: c_int;
    let rbitlen: c_int;
    let mut len: c_int;
    let ishift: c_int;
    let mut i: c_int;
    let mut e: int32 = 0;
    let s1: int32;
    let e1: int32;
    let mut r: *mut bits8;
    let mut ps: *mut bits8;

    bitlen = VARBITLEN(arg);
    s1 = Max(s, 1);
    /* If we do not have an upper bound, use end of string */
    if length_not_specified {
        e1 = bitlen + 1;
    } else if l < 0 {
        /* SQL99 says to throw an error for E < S, i.e., negative length */
        let _ = errcode(ERRCODE_SUBSTRING_ERROR);
        ereport!(ERROR, errmsg!("negative substring length not allowed"));
        e1 = -1; /* silence stupider compilers */
    } else if pg_add_s32_overflow(s, l, &mut e) {
        /*
         * L could be large enough for S + L to overflow, in which case the
         * substring must run to end of string.
         */
        e1 = bitlen + 1;
    } else {
        e1 = Min(e, bitlen + 1);
    }
    if s1 > bitlen || e1 <= s1 {
        /* Need to return a zero-length bitstring */
        len = VARBITTOTALLEN(0);
        result = palloc(len as Size) as *mut VarBit;
        SET_VARSIZE(result as *mut c_char, len);
        set_VARBITLEN(result, 0);
    } else {
        /*
         * OK, we've got a true substring starting at position s1-1 and ending
         * at position e1-1
         */
        rbitlen = e1 - s1;
        len = VARBITTOTALLEN(rbitlen);
        result = palloc(len as Size) as *mut VarBit;
        SET_VARSIZE(result as *mut c_char, len);
        set_VARBITLEN(result, rbitlen);
        len -= VARHDRSZ + VARBITHDRSZ as c_int;
        /* Are we copying from a byte boundary? */
        if (s1 - 1) % BITS_PER_BYTE == 0 {
            /* Yep, we are copying bytes */
            memcpy(
                VARBITS(result) as *mut c_void,
                VARBITS(arg).add(((s1 - 1) / BITS_PER_BYTE) as usize) as *const c_void,
                len as usize,
            );
        } else {
            /* Figure out how much we need to shift the sequence by */
            ishift = (s1 - 1) % BITS_PER_BYTE;
            r = VARBITS(result);
            ps = VARBITS(arg).add(((s1 - 1) / BITS_PER_BYTE) as usize);
            i = 0;
            while i < len {
                *r = (*ps << ishift) & BITMASK;
                ps = ps.add(1);
                if ps < VARBITEND(arg) {
                    *r |= *ps >> (BITS_PER_BYTE - ishift);
                }
                r = r.add(1);
                i += 1;
            }
        }

        /* Make sure last byte is correctly zero-padded */
        VARBIT_PAD(result);
    }

    result
}

/*
 * bitoverlay
 *	Replace specified substring of first string with second
 */
pub unsafe fn bitoverlay(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let t2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let sp: c_int = PG_GETARG_INT32!(fcinfo, 2); /* substring start position */
    let sl: c_int = PG_GETARG_INT32!(fcinfo, 3); /* substring length */

    return VarBitPGetDatum(bit_overlay(t1, t2, sp, sl)); // PG_RETURN_VARBIT_P
}

pub unsafe fn bitoverlay_no_len(fcinfo: FunctionCallInfo) -> Datum {
    let t1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let t2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let sp: c_int = PG_GETARG_INT32!(fcinfo, 2); /* substring start position */
    let sl: c_int;

    sl = VARBITLEN(t2); /* defaults to length(t2) */
    return VarBitPGetDatum(bit_overlay(t1, t2, sp, sl)); // PG_RETURN_VARBIT_P
}

unsafe fn bit_overlay(t1: *mut VarBit, t2: *mut VarBit, sp: c_int, sl: c_int) -> *mut VarBit {
    let mut result: *mut VarBit;
    let s1: *mut VarBit;
    let s2: *mut VarBit;
    let mut sp_pl_sl: c_int = 0;

    /*
     * Check for possible integer-overflow cases.  For negative sp, throw a
     * "substring length" error because that's what should be expected
     * according to the spec's definition of OVERLAY().
     */
    if sp <= 0 {
        let _ = errcode(ERRCODE_SUBSTRING_ERROR);
        ereport!(ERROR, errmsg!("negative substring length not allowed"));
    }
    if pg_add_s32_overflow(sp, sl, &mut sp_pl_sl) {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("integer out of range"));
    }

    s1 = bitsubstring(t1, 1, sp - 1, false);
    s2 = bitsubstring(t1, sp_pl_sl, -1, true);
    result = bit_catenate(s1, t2);
    result = bit_catenate(result, s2);

    result
}

/*
 * bit_count
 *
 * Returns the number of bits set in a bit string.
 */
pub unsafe fn bit_bit_count(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));

    PG_RETURN_INT64!(pg_popcount(VARBITS(arg) as *const c_char, VARBITBYTES(arg)) as int64);
}

/*
 * bitlength, bitoctetlength
 * Return the length of a bit string
 */
pub unsafe fn bitlength(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));

    PG_RETURN_INT32!(VARBITLEN(arg));
}

pub unsafe fn bitoctetlength(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));

    PG_RETURN_INT32!(VARBITBYTES(arg));
}

/*
 * bit_and
 * perform a logical AND on two bit strings.
 */
pub unsafe fn bit_and(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: *mut VarBit;
    let len: c_int;
    let bitlen1: c_int;
    let bitlen2: c_int;
    let mut i: c_int;
    let mut p1: *mut bits8;
    let mut p2: *mut bits8;
    let mut r: *mut bits8;

    bitlen1 = VARBITLEN(arg1);
    bitlen2 = VARBITLEN(arg2);
    if bitlen1 != bitlen2 {
        let _ = errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH);
        ereport!(ERROR, errmsg!("cannot AND bit strings of different sizes"));
    }

    len = VARSIZE(arg1 as *const c_char) as c_int;
    result = palloc(len as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, len);
    set_VARBITLEN(result, bitlen1);

    p1 = VARBITS(arg1);
    p2 = VARBITS(arg2);
    r = VARBITS(result);
    i = 0;
    while i < VARBITBYTES(arg1) {
        *r = *p1 & *p2;
        r = r.add(1);
        p1 = p1.add(1);
        p2 = p2.add(1);
        i += 1;
    }

    /* Padding is not needed as & of 0 pads is 0 */

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 * bit_or
 * perform a logical OR on two bit strings.
 */
pub unsafe fn bit_or(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: *mut VarBit;
    let len: c_int;
    let bitlen1: c_int;
    let bitlen2: c_int;
    let mut i: c_int;
    let mut p1: *mut bits8;
    let mut p2: *mut bits8;
    let mut r: *mut bits8;

    bitlen1 = VARBITLEN(arg1);
    bitlen2 = VARBITLEN(arg2);
    if bitlen1 != bitlen2 {
        let _ = errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH);
        ereport!(ERROR, errmsg!("cannot OR bit strings of different sizes"));
    }
    len = VARSIZE(arg1 as *const c_char) as c_int;
    result = palloc(len as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, len);
    set_VARBITLEN(result, bitlen1);

    p1 = VARBITS(arg1);
    p2 = VARBITS(arg2);
    r = VARBITS(result);
    i = 0;
    while i < VARBITBYTES(arg1) {
        *r = *p1 | *p2;
        r = r.add(1);
        p1 = p1.add(1);
        p2 = p2.add(1);
        i += 1;
    }

    /* Padding is not needed as | of 0 pads is 0 */

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 * bitxor
 * perform a logical XOR on two bit strings.
 */
pub unsafe fn bitxor(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let arg2: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let result: *mut VarBit;
    let len: c_int;
    let bitlen1: c_int;
    let bitlen2: c_int;
    let mut i: c_int;
    let mut p1: *mut bits8;
    let mut p2: *mut bits8;
    let mut r: *mut bits8;

    bitlen1 = VARBITLEN(arg1);
    bitlen2 = VARBITLEN(arg2);
    if bitlen1 != bitlen2 {
        let _ = errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH);
        ereport!(ERROR, errmsg!("cannot XOR bit strings of different sizes"));
    }

    len = VARSIZE(arg1 as *const c_char) as c_int;
    result = palloc(len as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, len);
    set_VARBITLEN(result, bitlen1);

    p1 = VARBITS(arg1);
    p2 = VARBITS(arg2);
    r = VARBITS(result);
    i = 0;
    while i < VARBITBYTES(arg1) {
        *r = *p1 ^ *p2;
        r = r.add(1);
        p1 = p1.add(1);
        p2 = p2.add(1);
        i += 1;
    }

    /* Padding is not needed as ^ of 0 pads is 0 */

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 * bitnot
 * perform a logical NOT on a bit string.
 */
pub unsafe fn bitnot(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let result: *mut VarBit;
    let mut p: *mut bits8;
    let mut r: *mut bits8;

    result = palloc(VARSIZE(arg as *const c_char) as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, VARSIZE(arg as *const c_char) as int32);
    set_VARBITLEN(result, VARBITLEN(arg));

    p = VARBITS(arg);
    r = VARBITS(result);
    while p < VARBITEND(arg) {
        *r = !*p;
        r = r.add(1);
        p = p.add(1);
    }

    /* Must zero-pad the result, because extra bits are surely 1's here */
    VARBIT_PAD_LAST(result, r);

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 * bitshiftleft
 * do a left shift (i.e. towards the beginning of the string)
 */
pub unsafe fn bitshiftleft(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut shft: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: *mut VarBit;
    let byte_shift: c_int;
    let ishift: c_int;
    let len: c_int;
    let mut p: *mut bits8;
    let mut r: *mut bits8;

    /* Negative shift is a shift to the right */
    if shft < 0 {
        /* Prevent integer overflow in negation */
        if shft < -VARBITMAXLEN {
            shft = -VARBITMAXLEN;
        }
        return DirectFunctionCall2Coll(
            bitshiftright,
            crate::postgres_ext::InvalidOid,
            VarBitPGetDatum(arg),
            Int32GetDatum(-shft),
        ); // PG_RETURN_DATUM
    }

    result = palloc(VARSIZE(arg as *const c_char) as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, VARSIZE(arg as *const c_char) as int32);
    set_VARBITLEN(result, VARBITLEN(arg));
    r = VARBITS(result);

    /* If we shifted all the bits out, return an all-zero string */
    if shft >= VARBITLEN(arg) {
        MemSet(r as *mut c_void, 0, VARBITBYTES(arg) as Size);
        return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
    }

    byte_shift = shft / BITS_PER_BYTE;
    ishift = shft % BITS_PER_BYTE;
    p = VARBITS(arg).add(byte_shift as usize);

    if ishift == 0 {
        /* Special case: we can do a memcpy */
        len = VARBITBYTES(arg) - byte_shift;
        memcpy(r as *mut c_void, p as *const c_void, len as usize);
        MemSet(r.add(len as usize) as *mut c_void, 0, byte_shift as Size);
    } else {
        while p < VARBITEND(arg) {
            *r = *p << ishift;
            p = p.add(1);
            if p < VARBITEND(arg) {
                *r |= *p >> (BITS_PER_BYTE - ishift);
            }
            r = r.add(1);
        }
        while r < VARBITEND(result) {
            *r = 0;
            r = r.add(1);
        }
    }

    /* The pad bits should be already zero at this point */

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 * bitshiftright
 * do a right shift (i.e. towards the end of the string)
 */
pub unsafe fn bitshiftright(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut shft: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: *mut VarBit;
    let byte_shift: c_int;
    let ishift: c_int;
    let len: c_int;
    let mut p: *mut bits8;
    let mut r: *mut bits8;

    /* Negative shift is a shift to the left */
    if shft < 0 {
        /* Prevent integer overflow in negation */
        if shft < -VARBITMAXLEN {
            shft = -VARBITMAXLEN;
        }
        return DirectFunctionCall2Coll(
            bitshiftleft,
            crate::postgres_ext::InvalidOid,
            VarBitPGetDatum(arg),
            Int32GetDatum(-shft),
        ); // PG_RETURN_DATUM
    }

    result = palloc(VARSIZE(arg as *const c_char) as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, VARSIZE(arg as *const c_char) as int32);
    set_VARBITLEN(result, VARBITLEN(arg));
    r = VARBITS(result);

    /* If we shifted all the bits out, return an all-zero string */
    if shft >= VARBITLEN(arg) {
        MemSet(r as *mut c_void, 0, VARBITBYTES(arg) as Size);
        return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
    }

    byte_shift = shft / BITS_PER_BYTE;
    ishift = shft % BITS_PER_BYTE;
    p = VARBITS(arg);

    /* Set the first part of the result to 0 */
    MemSet(r as *mut c_void, 0, byte_shift as Size);
    r = r.add(byte_shift as usize);

    if ishift == 0 {
        /* Special case: we can do a memcpy */
        len = VARBITBYTES(arg) - byte_shift;
        memcpy(r as *mut c_void, p as *const c_void, len as usize);
        r = r.add(len as usize);
    } else {
        if r < VARBITEND(result) {
            *r = 0; /* initialize first byte */
        }
        while r < VARBITEND(result) {
            *r |= *p >> ishift;
            r = r.add(1);
            if r < VARBITEND(result) {
                *r = (*p << (BITS_PER_BYTE - ishift)) & BITMASK;
            }
            p = p.add(1);
        }
    }

    /* We may have shifted 1's into the pad bits, so fix that */
    VARBIT_PAD_LAST(result, r);

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 * This is not defined in any standard. We retain the natural ordering of
 * bits here, as it just seems more intuitive.
 */
pub unsafe fn bitfromint4(fcinfo: FunctionCallInfo) -> Datum {
    let a: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let mut typmod: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: *mut VarBit;
    let mut r: *mut bits8;
    let rlen: c_int;
    let mut destbitsleft: c_int;
    let mut srcbitsleft: c_int;

    if typmod <= 0 || typmod > VARBITMAXLEN {
        typmod = 1; /* default bit length */
    }

    rlen = VARBITTOTALLEN(typmod);
    result = palloc(rlen as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, rlen);
    set_VARBITLEN(result, typmod);

    r = VARBITS(result);
    destbitsleft = typmod;
    srcbitsleft = 32;
    /* drop any input bits that don't fit */
    srcbitsleft = Min(srcbitsleft, destbitsleft);
    /* sign-fill any excess bytes in output */
    while destbitsleft >= srcbitsleft + 8 {
        *r = (if a < 0 { BITMASK as c_int } else { 0 }) as bits8;
        r = r.add(1);
        destbitsleft -= 8;
    }
    /* store first fractional byte */
    if destbitsleft > srcbitsleft {
        let mut val: c_uint = (a >> (destbitsleft - 8)) as c_uint;

        /* Force sign-fill in case the compiler implements >> as zero-fill */
        if a < 0 {
            val |= ((-1i32) as c_uint) << (srcbitsleft + 8 - destbitsleft);
        }
        *r = (val & BITMASK as c_uint) as bits8;
        r = r.add(1);
        destbitsleft -= 8;
    }
    /* Now srcbitsleft and destbitsleft are the same, need not track both */
    /* store whole bytes */
    while destbitsleft >= 8 {
        *r = ((a >> (destbitsleft - 8)) & BITMASK as c_int) as bits8;
        r = r.add(1);
        destbitsleft -= 8;
    }
    /* store last fractional byte */
    if destbitsleft > 0 {
        *r = ((a << (8 - destbitsleft)) & BITMASK as c_int) as bits8;
    }

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

pub unsafe fn bittoint4(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut result: uint32;
    let mut r: *mut bits8;

    /* Check that the bit string is not too long */
    if VARBITLEN(arg) > (core::mem::size_of::<uint32>() as c_int) * BITS_PER_BYTE {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("integer out of range"));
    }

    result = 0;
    r = VARBITS(arg);
    while r < VARBITEND(arg) {
        result <<= BITS_PER_BYTE;
        result |= *r as uint32;
        r = r.add(1);
    }
    /* Now shift the result to take account of the padding at the end */
    result >>= VARBITPAD(arg);

    PG_RETURN_INT32!(result as int32);
}

pub unsafe fn bitfromint8(fcinfo: FunctionCallInfo) -> Datum {
    let a: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let mut typmod: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: *mut VarBit;
    let mut r: *mut bits8;
    let rlen: c_int;
    let mut destbitsleft: c_int;
    let mut srcbitsleft: c_int;

    if typmod <= 0 || typmod > VARBITMAXLEN {
        typmod = 1; /* default bit length */
    }

    rlen = VARBITTOTALLEN(typmod);
    result = palloc(rlen as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, rlen);
    set_VARBITLEN(result, typmod);

    r = VARBITS(result);
    destbitsleft = typmod;
    srcbitsleft = 64;
    /* drop any input bits that don't fit */
    srcbitsleft = Min(srcbitsleft, destbitsleft);
    /* sign-fill any excess bytes in output */
    while destbitsleft >= srcbitsleft + 8 {
        *r = (if a < 0 { BITMASK as c_int } else { 0 }) as bits8;
        r = r.add(1);
        destbitsleft -= 8;
    }
    /* store first fractional byte */
    if destbitsleft > srcbitsleft {
        // NB: matches C exactly - the temporary is a 32-bit `unsigned int`.
        let mut val: c_uint = (a >> (destbitsleft - 8)) as c_uint;

        /* Force sign-fill in case the compiler implements >> as zero-fill */
        if a < 0 {
            val |= ((-1i32) as c_uint) << (srcbitsleft + 8 - destbitsleft);
        }
        *r = (val & BITMASK as c_uint) as bits8;
        r = r.add(1);
        destbitsleft -= 8;
    }
    /* Now srcbitsleft and destbitsleft are the same, need not track both */
    /* store whole bytes */
    while destbitsleft >= 8 {
        *r = ((a >> (destbitsleft - 8)) & BITMASK as int64) as bits8;
        r = r.add(1);
        destbitsleft -= 8;
    }
    /* store last fractional byte */
    if destbitsleft > 0 {
        *r = ((a << (8 - destbitsleft)) & BITMASK as int64) as bits8;
    }

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

pub unsafe fn bittoint8(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let mut result: uint64;
    let mut r: *mut bits8;

    /* Check that the bit string is not too long */
    if VARBITLEN(arg) > (core::mem::size_of::<uint64>() as c_int) * BITS_PER_BYTE {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("bigint out of range"));
    }

    result = 0;
    r = VARBITS(arg);
    while r < VARBITEND(arg) {
        result <<= BITS_PER_BYTE;
        result |= *r as uint64;
        r = r.add(1);
    }
    /* Now shift the result to take account of the padding at the end */
    result >>= VARBITPAD(arg);

    PG_RETURN_INT64!(result as int64);
}

/*
 * Determines the position of S2 in the bitstring S1 (1-based string).
 * If S2 does not appear in S1 this function returns 0.
 * If S2 is of length 0 this function returns 1.
 * Compatible in usage with POSITION() functions for other data types.
 */
pub unsafe fn bitposition(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let substr: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 1));
    let substr_length: c_int;
    let str_length: c_int;
    let mut i: c_int;
    let mut is: c_int;
    let mut s: *mut bits8; /* pointer into substring */
    let mut p: *mut bits8; /* pointer into str */
    let mut cmp: bits8; /* shifted substring byte to compare */
    let mut mask1: bits8; /* mask for substring byte shifted right */
    let mut mask2: bits8; /* mask for substring byte shifted left */
    let end_mask: bits8; /* pad mask for last substring byte */
    let str_mask: bits8; /* pad mask for last string byte */
    let mut is_match: bool;

    /* Get the substring length */
    substr_length = VARBITLEN(substr);
    str_length = VARBITLEN(str);

    /* String has zero length or substring longer than string, return 0 */
    if str_length == 0 || substr_length > str_length {
        PG_RETURN_INT32!(0);
    }

    /* zero-length substring means return 1 */
    if substr_length == 0 {
        PG_RETURN_INT32!(1);
    }

    /* Initialise the padding masks */
    end_mask = BITMASK << VARBITPAD(substr);
    str_mask = BITMASK << VARBITPAD(str);
    i = 0;
    while i < VARBITBYTES(str) - VARBITBYTES(substr) + 1 {
        is = 0;
        while is < BITS_PER_BYTE {
            is_match = true;
            p = VARBITS(str).add(i as usize);
            mask1 = BITMASK >> is;
            mask2 = !mask1;
            s = VARBITS(substr);
            while is_match && s < VARBITEND(substr) {
                cmp = *s >> is;
                if s == VARBITEND(substr).sub(1) {
                    mask1 &= end_mask >> is;
                    if p == VARBITEND(str).sub(1) {
                        /* Check that there is enough of str left */
                        if (mask1 & !str_mask) != 0 {
                            is_match = false;
                            break;
                        }
                        mask1 &= str_mask;
                    }
                }
                is_match = ((cmp ^ *p) & mask1) == 0;
                if !is_match {
                    break;
                }
                /* Move on to the next byte */
                p = p.add(1);
                if p == VARBITEND(str) {
                    // C promotes end_mask (uint8) to int before the shift, so a
                    // shift of 8 (is == 0) is well-defined and truncates to 0.
                    mask2 = ((end_mask as c_int) << (BITS_PER_BYTE - is)) as bits8;
                    is_match = mask2 == 0;
                    break;
                }
                cmp = ((*s as c_int) << (BITS_PER_BYTE - is)) as bits8;
                if s == VARBITEND(substr).sub(1) {
                    mask2 &= ((end_mask as c_int) << (BITS_PER_BYTE - is)) as bits8;
                    if p == VARBITEND(str).sub(1) {
                        if (mask2 & !str_mask) != 0 {
                            is_match = false;
                            break;
                        }
                        mask2 &= str_mask;
                    }
                }
                is_match = ((cmp ^ *p) & mask2) == 0;
                s = s.add(1);
            }
            /* Have we found a match? */
            if is_match {
                PG_RETURN_INT32!(i * BITS_PER_BYTE + is + 1);
            }
            is += 1;
        }
        i += 1;
    }
    PG_RETURN_INT32!(0);
}

/*
 * bitsetbit
 *
 * Given an instance of type 'bit' creates a new one with
 * the Nth bit set to the given value.
 */
pub unsafe fn bitsetbit(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let n: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let newBit: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut VarBit;
    let len: c_int;
    let bitlen: c_int;
    let r: *mut bits8;
    let p: *mut bits8;
    let byteNo: c_int;
    let bitNo: c_int;

    bitlen = VARBITLEN(arg1);
    if n < 0 || n >= bitlen {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(
            ERROR,
            errmsg!("bit index {} out of valid range (0..{})", n, bitlen - 1)
        );
    }

    /*
     * sanity check!
     */
    if newBit != 0 && newBit != 1 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("new bit must be 0 or 1"));
    }

    len = VARSIZE(arg1 as *const c_char) as c_int;
    result = palloc(len as Size) as *mut VarBit;
    SET_VARSIZE(result as *mut c_char, len);
    set_VARBITLEN(result, bitlen);

    p = VARBITS(arg1);
    r = VARBITS(result);

    memcpy(r as *mut c_void, p as *const c_void, VARBITBYTES(arg1) as usize);

    byteNo = n / BITS_PER_BYTE;
    bitNo = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE);

    /*
     * Update the byte.
     */
    if newBit == 0 {
        *r.add(byteNo as usize) &= !((1 << bitNo) as bits8);
    } else {
        *r.add(byteNo as usize) |= (1 << bitNo) as bits8;
    }

    return VarBitPGetDatum(result); // PG_RETURN_VARBIT_P
}

/*
 * bitgetbit
 *
 * returns the value of the Nth bit of a bit array (0 or 1).
 */
pub unsafe fn bitgetbit(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: *mut VarBit = DatumGetVarBitP(PG_GETARG_DATUM!(fcinfo, 0));
    let n: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let bitlen: c_int;
    let p: *mut bits8;
    let byteNo: c_int;
    let bitNo: c_int;

    bitlen = VARBITLEN(arg1);
    if n < 0 || n >= bitlen {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(
            ERROR,
            errmsg!("bit index {} out of valid range (0..{})", n, bitlen - 1)
        );
    }

    p = VARBITS(arg1);

    byteNo = n / BITS_PER_BYTE;
    bitNo = BITS_PER_BYTE - 1 - (n % BITS_PER_BYTE);

    if (*p.add(byteNo as usize) & ((1 << bitNo) as bits8)) != 0 {
        PG_RETURN_INT32!(1);
    } else {
        PG_RETURN_INT32!(0);
    }
}

// ----------------------------------------------------------------
//   small helpers
// ----------------------------------------------------------------

// libc strlen (string.h, via postgres.h).
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * Render the first character of a C string for an error message.
 *
 * C uses pg_mblen_cstr(sp) + "%.*s" to print the (possibly multibyte) offending
 * character.  pg_mblen / mb support is not yet ported, so we print a single byte
 * (lossy) which is correct for the ASCII bit/hex inputs this type accepts.
 *
 * # Safety
 * `sp` is a valid NUL-terminated C string positioned at the offending char.
 */
unsafe fn mblen_str(sp: *const c_char) -> std::string::String {
    // TODO(pg-port): pg_mblen_cstr (mb/mbutils) not yet translated; print one byte.
    let b = *sp as u8;
    std::string::String::from_utf8_lossy(&[b]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{
        CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt32, Int32GetDatum,
    };
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{
        DirectFunctionCall1Coll, DirectFunctionCall2Coll, DirectFunctionCall3Coll,
    };

    // bit_in / varbit_in take (cstring, typelem, atttypmod); typmod = -1 (not supplied).
    unsafe fn in_bit(s: &core::ffi::CStr) -> Datum {
        DirectFunctionCall3Coll(
            bit_in,
            InvalidOid,
            CStringGetDatum(s.as_ptr()),
            0, /* typelem (unused) */
            Int32GetDatum(-1),
        )
    }
    unsafe fn in_varbit(s: &core::ffi::CStr) -> Datum {
        DirectFunctionCall3Coll(
            varbit_in,
            InvalidOid,
            CStringGetDatum(s.as_ptr()),
            0,
            Int32GetDatum(-1),
        )
    }

    unsafe fn cstr_eq(p: *mut c_char, want: &str) -> bool {
        let n = strlen(p);
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn bit_io_logic_cat_compare() {
        unsafe {
            // bit_in "101" -> bit_out "101"
            let d = in_bit(c"101");
            let s = DatumGetCString(DirectFunctionCall1Coll(bit_out, InvalidOid, d));
            assert!(cstr_eq(s, "101"));
            // inspect the VarBit: bitlen 3, first byte 0b101_00000
            let vb = DatumGetVarBitP(d);
            assert_eq!(VARBITLEN(vb), 3);
            assert_eq!(*VARBITS(vb), 0b1010_0000);

            // varbit_in / varbit_out round trip
            let dv = in_varbit(c"11001010");
            let sv = DatumGetCString(DirectFunctionCall1Coll(varbit_out, InvalidOid, dv));
            assert!(cstr_eq(sv, "11001010"));

            // hex input parses to the same value as binary
            let dh = in_bit(c"xC");
            let sh = DatumGetCString(DirectFunctionCall1Coll(bit_out, InvalidOid, dh));
            assert!(cstr_eq(sh, "1100"));

            // bitand / bitor / bitnot of two equal-length strings
            let a = in_bit(c"1100");
            let b = in_bit(c"1010");
            let and = DatumGetCString(DirectFunctionCall1Coll(
                bit_out,
                InvalidOid,
                DirectFunctionCall2Coll(bit_and, InvalidOid, a, b),
            ));
            assert!(cstr_eq(and, "1000"));
            let or = DatumGetCString(DirectFunctionCall1Coll(
                bit_out,
                InvalidOid,
                DirectFunctionCall2Coll(bit_or, InvalidOid, a, b),
            ));
            assert!(cstr_eq(or, "1110"));
            let xor = DatumGetCString(DirectFunctionCall1Coll(
                bit_out,
                InvalidOid,
                DirectFunctionCall2Coll(bitxor, InvalidOid, a, b),
            ));
            assert!(cstr_eq(xor, "0110"));
            let not = DatumGetCString(DirectFunctionCall1Coll(
                bit_out,
                InvalidOid,
                DirectFunctionCall1Coll(bitnot, InvalidOid, a),
            ));
            assert!(cstr_eq(not, "0011"));

            // bitcat: "1100" || "1010" == "11001010"
            let cat = DatumGetCString(DirectFunctionCall1Coll(
                bit_out,
                InvalidOid,
                DirectFunctionCall2Coll(bitcat, InvalidOid, a, b),
            ));
            assert!(cstr_eq(cat, "11001010"));

            // biteq / bitne / bitcmp ordering: "1100" < "1110"
            let c = in_bit(c"1110");
            assert!(DatumGetBool(DirectFunctionCall2Coll(biteq, InvalidOid, a, a)));
            assert!(DatumGetBool(DirectFunctionCall2Coll(bitne, InvalidOid, a, c)));
            assert!(DatumGetInt32(DirectFunctionCall2Coll(bitcmp, InvalidOid, a, c)) < 0);
            assert!(DatumGetInt32(DirectFunctionCall2Coll(bitcmp, InvalidOid, c, a)) > 0);
            assert_eq!(DatumGetInt32(DirectFunctionCall2Coll(bitcmp, InvalidOid, a, a)), 0);

            // bitlength / bitoctetlength
            assert_eq!(DatumGetInt32(DirectFunctionCall1Coll(bitlength, InvalidOid, a)), 4);
            assert_eq!(DatumGetInt32(DirectFunctionCall1Coll(bitoctetlength, InvalidOid, a)), 1);

            // bitgetbit: bit 0 of "1100" is 1, bit 2 is 0
            assert_eq!(
                DatumGetInt32(DirectFunctionCall2Coll(bitgetbit, InvalidOid, a, Int32GetDatum(0))),
                1
            );
            assert_eq!(
                DatumGetInt32(DirectFunctionCall2Coll(bitgetbit, InvalidOid, a, Int32GetDatum(2))),
                0
            );
        }
    }

    #[test]
    fn bitfromint4_roundtrip() {
        unsafe {
            // bitfromint4(5, 32) -> bittoint4 == 5
            let bits = DirectFunctionCall2Coll(
                bitfromint4,
                InvalidOid,
                Int32GetDatum(5),
                Int32GetDatum(32),
            );
            let back = DatumGetInt32(DirectFunctionCall1Coll(bittoint4, InvalidOid, bits));
            assert_eq!(back, 5);

            // bittoint8 of the same 32-bit string also yields 5
            let back8 = DirectFunctionCall1Coll(bittoint8, InvalidOid, bits);
            assert_eq!(DatumGetInt64(back8), 5);
        }
    }

    #[test]
    fn bitshift_roundtrip() {
        unsafe {
            // "11000000" << 2 == "00000011" is NOT it; left shift moves toward start.
            let v = in_bit(c"00010000");
            let l = DatumGetCString(DirectFunctionCall1Coll(
                bit_out,
                InvalidOid,
                DirectFunctionCall2Coll(bitshiftleft, InvalidOid, v, Int32GetDatum(3)),
            ));
            assert!(cstr_eq(l, "10000000"));
            let r = DatumGetCString(DirectFunctionCall1Coll(
                bit_out,
                InvalidOid,
                DirectFunctionCall2Coll(bitshiftright, InvalidOid, v, Int32GetDatum(2)),
            ));
            assert!(cstr_eq(r, "00000100"));
        }
    }

    #[test]
    #[should_panic]
    fn bit_in_rejects_garbage() {
        unsafe {
            in_bit(c"10201");
        }
    }
}
