//! src/backend/utils/adt/tsgistidx.c
//!
//! GiST support functions for tsvector_ops (the signature-tree opclass).
//!
//! #include mapping:
//!   postgres.h                  -> crate::prelude::*
//!   access/gist.h               -> NOT PORTED.  GISTENTRY / GistEntryVector /
//!                                  GIST_SPLITVEC / gistentryinit are defined
//!                                  LOCALLY below (mirrors tsquery_gist.rs).
//!                                  TODO(pg-port): replace with crate::access::gist.
//!   access/heaptoast.h          -> only TOAST_INDEX_TARGET (declared LOCALLY,
//!                                  matching access::common::indextuple = 510).
//!   access/reloptions.h         -> init_local_reloptions / add_local_int_reloption
//!                                  + local_relopts are NOT PORTED -> gtsvector_options
//!                                  is STUBBED.
//!   common/int.h                -> crate::common::int (pg_cmp_s32)
//!   lib/qunique.h               -> crate::lib::qunique (qunique)
//!   port/pg_bitutils.h          -> crate::port::pg_bitutils (pg_popcount,
//!                                  pg_number_of_ones)
//!   tsearch/ts_utils.h          -> TSQuery/QueryItem/QueryOperand/GETQUERY from
//!                                  tsquery_util; TS_execute / ExecPhraseData /
//!                                  TSTernaryValue / TS_EXEC_PHRASE_NO_POS from
//!                                  tsvector_op.
//!   utils/fmgrprotos.h          -> crate::utils::fmgr (FunctionCallInfo) + fmgr macros
//!   utils/pg_crc.h              -> crate::utils::hash::pg_crc (LEGACY CRC32)
//!
//! REAL: the entire signature-bit core -- SIGLEN/SIGLENBIT bit math
//! (GETBIT/SETBIT/HASHVAL), makesign, sizebitvec(popcount), hemdistsign(Hamming),
//! unionkey (bitwise-OR of child sigs / HASH of array entries), gtsvector_union,
//! gtsvector_same, gtsvector_penalty, gtsvector_picksplit (the seeds + WISH_F cost
//! model), and the gtsvector_compress array-build (CRC per lexeme, sort+qunique,
//! makesign-if-too-long).  gtsvector_consistent runs the REAL TS_execute engine
//! with the REAL checkcondition_arr / checkcondition_bit callbacks.
//!
//! STUBBED: gtsvector_compress / gtsvector_decompress palloc a fresh GISTENTRY
//! and use entry->rel/page/offset/leafkey -- that plumbing is real here, but the
//! PG_DETOAST_DATUM path on a stored toasted key is exercised only at runtime.
//! gtsvector_options needs the unported reloptions framework -> unimplemented!().

use crate::prelude::*;

use crate::common::int::pg_cmp_s32;
use crate::lib::qunique::qunique;
use crate::port::pg_bitutils::{pg_number_of_ones, pg_popcount};
use crate::storage::off::{FirstOffsetNumber, OffsetNumber, OffsetNumberNext};
use crate::utils::adt::tsquery_util::{GETQUERY, QueryOperand, TSQuery};
use crate::utils::adt::tsvector::{ARRPTR, DatumGetTSVector, STRPTR, TSVector};
use crate::utils::adt::tsvector_op::{
    ExecPhraseData, TSTernaryValue, TS_EXEC_PHRASE_NO_POS, TS_MAYBE, TS_NO, TS_execute,
};
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::hash::pg_crc::{
    pg_crc32, COMP_LEGACY_CRC32, FIN_LEGACY_CRC32, INIT_LEGACY_CRC32,
};

use crate::{
    PG_DETOAST_DATUM, PG_FREE_IF_COPY, PG_GETARG_DATUM, PG_GETARG_POINTER, PG_GETARG_TSQUERY,
    PG_RETURN_BOOL, PG_RETURN_POINTER, PG_RETURN_VOID,
};

// ================================================================
//   tsvector_ops opclass options + signature constants
// ================================================================

/*
 * typedef struct { int32 vl_len_; int siglen; } GistTsVectorOptions;
 */
#[repr(C)]
pub struct GistTsVectorOptions {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub siglen: c_int,  /* signature length */
}

/* #define SIGLEN_DEFAULT (31 * 4) */
const SIGLEN_DEFAULT: c_int = 31 * 4;

/*
 * #define GET_SIGLEN() ... PG_GET_OPCLASS_OPTIONS()->siglen : SIGLEN_DEFAULT
 *
 * The opclass-options machinery (PG_HAS_OPCLASS_OPTIONS) is not ported; GiST
 * always passes the default when no options are set, which is what we model.
 */
#[inline]
unsafe fn GET_SIGLEN() -> c_int {
    // TODO(pg-port): honor PG_GET_OPCLASS_OPTIONS()->siglen once reloptions is ported.
    SIGLEN_DEFAULT
}

/* #define BITS_PER_BYTE 8 (c.h) */
const BITS_PER_BYTE: c_int = 8;

/* #define SIGLENBIT(siglen) ((siglen) * BITS_PER_BYTE) */
#[inline]
fn SIGLENBIT(siglen: c_int) -> c_int {
    siglen * BITS_PER_BYTE
}

/* typedef char *BITVECP; */
type BITVECP = *mut c_char;

/* #define GETBYTE(x,i) ( *( (BITVECP)(x) + (int)( (i) / BITS_PER_BYTE ) ) ) */
#[inline]
unsafe fn GETBYTE(x: BITVECP, i: c_int) -> c_char {
    *x.add((i / BITS_PER_BYTE) as usize)
}

/* #define SETBIT(x,i) GETBYTE(x,i) |= ( 0x01 << ( (i) % BITS_PER_BYTE ) ) */
#[inline]
unsafe fn SETBIT(x: BITVECP, i: c_int) {
    let p = x.add((i / BITS_PER_BYTE) as usize);
    *p = (*p as u8 | (0x01u8 << ((i % BITS_PER_BYTE) as u8))) as c_char;
}

/* #define GETBIT(x,i) ( (GETBYTE(x,i) >> ( (i) % BITS_PER_BYTE )) & 0x01 ) */
#[inline]
unsafe fn GETBIT(x: BITVECP, i: c_int) -> c_int {
    ((GETBYTE(x, i) as u8 >> ((i % BITS_PER_BYTE) as u8)) & 0x01) as c_int
}

/* #define HASHVAL(val, siglen) (((unsigned int)(val)) % SIGLENBIT(siglen)) */
#[inline]
fn HASHVAL(val: int32, siglen: c_int) -> c_int {
    ((val as u32) % (SIGLENBIT(siglen) as u32)) as c_int
}

/* #define HASH(sign, val, siglen) SETBIT((sign), HASHVAL(val, siglen)) */
#[inline]
unsafe fn HASH(sign: BITVECP, val: int32, siglen: c_int) {
    SETBIT(sign, HASHVAL(val, siglen));
}

// ================================================================
//   SignTSVector: the GiST index key (a varlena)
// ================================================================

/*
 * typedef struct { int32 vl_len_; int32 flag; char data[FLEXIBLE_ARRAY_MEMBER]; }
 *     SignTSVector;
 */
#[repr(C)]
pub struct SignTSVector {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub flag: int32,
    pub data: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

const ARRKEY: int32 = 0x01;
const SIGNKEY: int32 = 0x02;
const ALLISTRUE: int32 = 0x04;

/* #define ISARRKEY(x) ( ((SignTSVector*)(x))->flag & ARRKEY ) */
#[inline]
unsafe fn ISARRKEY(x: *const SignTSVector) -> bool {
    (*x).flag & ARRKEY != 0
}
/* #define ISSIGNKEY(x) ( ((SignTSVector*)(x))->flag & SIGNKEY ) */
#[inline]
unsafe fn ISSIGNKEY(x: *const SignTSVector) -> bool {
    (*x).flag & SIGNKEY != 0
}
/* #define ISALLTRUE(x) ( ((SignTSVector*)(x))->flag & ALLISTRUE ) */
#[inline]
unsafe fn ISALLTRUE(x: *const SignTSVector) -> bool {
    (*x).flag & ALLISTRUE != 0
}

/* #define GTHDRSIZE ( VARHDRSZ + sizeof(int32) ) */
#[inline]
fn GTHDRSIZE() -> usize {
    VARHDRSZ as usize + core::mem::size_of::<int32>()
}

/*
 * #define CALCGTSIZE(flag, len) ( GTHDRSIZE +
 *     ( (flag & ARRKEY) ? len*sizeof(int32)
 *       : ((flag & ALLISTRUE) ? 0 : len) ) )
 */
#[inline]
fn CALCGTSIZE(flag: int32, len: c_int) -> usize {
    GTHDRSIZE()
        + if flag & ARRKEY != 0 {
            (len as usize) * core::mem::size_of::<int32>()
        } else if flag & ALLISTRUE != 0 {
            0
        } else {
            len as usize
        }
}

/* #define GETSIGN(x) ( (BITVECP)( (char*)(x)+GTHDRSIZE ) ) */
#[inline]
unsafe fn GETSIGN(x: *mut SignTSVector) -> BITVECP {
    (x as *mut c_char).add(GTHDRSIZE())
}

/* #define GETSIGLEN(x)( VARSIZE(x) - GTHDRSIZE ) */
#[inline]
unsafe fn GETSIGLEN(x: *mut SignTSVector) -> c_int {
    (crate::varatt::VARSIZE(x as *const c_char) as usize - GTHDRSIZE()) as c_int
}

/* #define GETARR(x) ( (int32*)( (char*)(x)+GTHDRSIZE ) ) */
#[inline]
unsafe fn GETARR(x: *mut SignTSVector) -> *mut int32 {
    (x as *mut c_char).add(GTHDRSIZE()) as *mut int32
}

/* #define ARRNELEM(x) ( ( VARSIZE(x) - GTHDRSIZE )/sizeof(int32) ) */
#[inline]
unsafe fn ARRNELEM(x: *mut SignTSVector) -> c_int {
    ((crate::varatt::VARSIZE(x as *const c_char) as usize - GTHDRSIZE())
        / core::mem::size_of::<int32>()) as c_int
}

// ================================================================
//   access/gist.h  --  NOT PORTED.  Minimal local definitions
//   (mirror of tsquery_gist.rs).  TODO(pg-port): use crate::access::gist.
// ================================================================

#[repr(C)]
pub struct GISTENTRY {
    pub key: Datum,
    pub rel: *mut c_void,
    pub page: *mut c_void,
    pub offset: OffsetNumber,
    pub leafkey: bool,
}

#[repr(C)]
pub struct GIST_SPLITVEC {
    pub spl_left: *mut OffsetNumber,
    pub spl_nleft: c_int,
    pub spl_ldatum: Datum,
    pub spl_ldatum_exists: bool,

    pub spl_right: *mut OffsetNumber,
    pub spl_nright: c_int,
    pub spl_rdatum: Datum,
    pub spl_rdatum_exists: bool,
}

#[repr(C)]
pub struct GistEntryVector {
    pub n: int32,
    pub vector: [GISTENTRY; 0],
}

impl GistEntryVector {
    /* &entryvec->vector[pos] */
    #[inline]
    unsafe fn entry(&self, pos: usize) -> *const GISTENTRY {
        self.vector.as_ptr().add(pos)
    }
}

/* #define gistentryinit(e, k, r, pg, o, l) ... */
#[inline]
unsafe fn gistentryinit(
    e: *mut GISTENTRY,
    k: Datum,
    r: *mut c_void,
    pg: *mut c_void,
    o: OffsetNumber,
    l: bool,
) {
    (*e).key = k;
    (*e).rel = r;
    (*e).page = pg;
    (*e).offset = o;
    (*e).leafkey = l;
}

/* #define GETENTRY(vec,pos) ((SignTSVector *) DatumGetPointer((vec)->vector[pos].key)) */
#[inline]
unsafe fn GETENTRY(vec: *const GistEntryVector, pos: usize) -> *mut SignTSVector {
    DatumGetPointer((*(*vec).entry(pos)).key) as *mut SignTSVector
}

/* access/heaptoast.h: TOAST_INDEX_TARGET (matches access::common::indextuple). */
const TOAST_INDEX_TARGET: u32 = 510;

// ================================================================
//   Functions
// ================================================================

/* static int32 sizebitvec(BITVECP sign, int siglen); -- forward decl in C. */

pub unsafe fn gtsvectorin(_fcinfo: FunctionCallInfo) -> Datum {
    /* There's no need to support input of gtsvectors */
    ereport!(ERROR, "cannot accept a value of type gtsvector");
    /* keep compiler quiet (ereport(ERROR) never returns) */
    #[allow(unreachable_code)]
    {
        PG_RETURN_VOID!()
    }
}

pub unsafe fn gtsvectorout(fcinfo: FunctionCallInfo) -> Datum {
    let key = PG_DETOAST_DATUM!(PG_GETARG_DATUM!(fcinfo, 0)) as *mut SignTSVector;
    let outbuf: *mut c_char;

    if ISARRKEY(key) {
        outbuf = cstr_printf_1d(c"%d unique words".as_ptr(), ARRNELEM(key));
    } else if ISALLTRUE(key) {
        outbuf = pstrdup(c"all true bits".as_ptr());
    } else {
        let siglen = GETSIGLEN(key);
        let cnttrue = sizebitvec(GETSIGN(key), siglen);

        outbuf = cstr_printf_2d(
            c"%d true bits, %d false bits".as_ptr(),
            cnttrue,
            SIGLENBIT(siglen) - cnttrue,
        );
    }

    PG_FREE_IF_COPY!(fcinfo, key, 0);
    PG_RETURN_POINTER!(outbuf)
}

/*
 * psprintf() is not yet ported.  These two helpers reproduce the exact format
 * strings via libc snprintf into a palloc'd buffer (same pattern as oid.rs).
 */
extern "C" {
    fn snprintf(buf: *mut c_char, size: Size, fmt: *const c_char, ...) -> c_int;
}

unsafe fn cstr_printf_1d(fmt: *const c_char, a: c_int) -> *mut c_char {
    /* "%d unique words": 32 bytes is ample for any int. */
    let buf = palloc(64) as *mut c_char;
    snprintf(buf, 64, fmt, a);
    buf
}

unsafe fn cstr_printf_2d(fmt: *const c_char, a: c_int, b: c_int) -> *mut c_char {
    let buf = palloc(96) as *mut c_char;
    snprintf(buf, 96, fmt, a, b);
    buf
}

/* static int compareint(const void *va, const void *vb) -> pg_cmp_s32. */
unsafe fn compareint(va: *const c_void, vb: *const c_void) -> c_int {
    let a = *(va as *const int32);
    let b = *(vb as *const int32);
    pg_cmp_s32(a, b)
}

/*
 * static void makesign(BITVECP sign, SignTSVector *a, int siglen)
 * Zero the signature then HASH every array entry into it.  REAL.
 */
unsafe fn makesign(sign: BITVECP, a: *mut SignTSVector, siglen: c_int) {
    let len = ARRNELEM(a);
    let ptr = GETARR(a);

    MemSet(sign as *mut c_void, 0, siglen as Size);
    for k in 0..len {
        HASH(sign, *ptr.add(k as usize), siglen);
    }
}

/*
 * static SignTSVector *gtsvector_alloc(int flag, int len, BITVECP sign)
 */
unsafe fn gtsvector_alloc(flag: int32, len: c_int, sign: BITVECP) -> *mut SignTSVector {
    let size = CALCGTSIZE(flag, len);
    let res = palloc(size) as *mut SignTSVector;

    crate::varatt::SET_VARSIZE(res as *mut c_char, size as int32);
    (*res).flag = flag;

    if (flag & (SIGNKEY | ALLISTRUE)) == SIGNKEY && !sign.is_null() {
        core::ptr::copy_nonoverlapping(sign, GETSIGN(res), len as usize);
    }

    res
}

pub unsafe fn gtsvector_compress(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let siglen = GET_SIGLEN();
    let mut retval = entry;

    if (*entry).leafkey {
        /* tsvector */
        let val: TSVector = DatumGetTSVector((*entry).key);
        let mut res = gtsvector_alloc(ARRKEY, (*val).size, null_mut());
        let mut len: int32;
        let mut arr: *mut int32;
        let mut ptr = ARRPTR(val);
        let words = STRPTR(val);

        arr = GETARR(res);
        len = (*val).size;
        while len != 0 {
            len -= 1;
            let mut c: pg_crc32 = INIT_LEGACY_CRC32();
            c = COMP_LEGACY_CRC32(
                c,
                words.add((*ptr).pos() as usize) as *const c_void,
                (*ptr).len(),
            );
            c = FIN_LEGACY_CRC32(c);

            *arr = c as int32;
            arr = arr.add(1);
            ptr = ptr.add(1);
        }

        crate::port::qsort::pg_qsort(
            GETARR(res) as *mut c_void,
            (*val).size as usize,
            core::mem::size_of::<c_int>(),
            compareint,
        );
        let nlen = qunique(
            GETARR(res) as *mut c_void,
            (*val).size as usize,
            core::mem::size_of::<c_int>(),
            compareint,
        ) as int32;
        if nlen != (*val).size {
            /*
             * there is a collision of hash-function; nlen is always less than
             * val->size
             */
            let newsize = CALCGTSIZE(ARRKEY, nlen);
            res = repalloc(res as *mut c_void, newsize) as *mut SignTSVector;
            crate::varatt::SET_VARSIZE(res as *mut c_char, newsize as int32);
        }

        /* make signature, if array is too long */
        if crate::varatt::VARSIZE(res as *const c_char) > TOAST_INDEX_TARGET {
            let ressign = gtsvector_alloc(SIGNKEY, siglen, null_mut());

            makesign(GETSIGN(ressign), res, siglen);
            res = ressign;
        }

        retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;
        gistentryinit(
            retval,
            PointerGetDatum(res as *const c_void),
            (*entry).rel,
            (*entry).page,
            (*entry).offset,
            false,
        );
    } else if ISSIGNKEY(DatumGetPointer((*entry).key) as *const SignTSVector)
        && !ISALLTRUE(DatumGetPointer((*entry).key) as *const SignTSVector)
    {
        let res: *mut SignTSVector;
        let sign = GETSIGN(DatumGetPointer((*entry).key) as *mut SignTSVector);

        for i in 0..siglen {
            if (*sign.add(i as usize) as u8) != 0xff {
                PG_RETURN_POINTER!(retval);
            }
        }

        res = gtsvector_alloc(SIGNKEY | ALLISTRUE, siglen, sign);
        retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;
        gistentryinit(
            retval,
            PointerGetDatum(res as *const c_void),
            (*entry).rel,
            (*entry).page,
            (*entry).offset,
            false,
        );
    }
    PG_RETURN_POINTER!(retval)
}

pub unsafe fn gtsvector_decompress(fcinfo: FunctionCallInfo) -> Datum {
    /*
     * We need to detoast the stored value, because the other gtsvector support
     * functions don't cope with toasted values.
     */
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let key = PG_DETOAST_DATUM!((*entry).key) as *mut SignTSVector;

    if key != DatumGetPointer((*entry).key) as *mut SignTSVector {
        let retval = palloc(core::mem::size_of::<GISTENTRY>()) as *mut GISTENTRY;

        gistentryinit(
            retval,
            PointerGetDatum(key as *const c_void),
            (*entry).rel,
            (*entry).page,
            (*entry).offset,
            false,
        );

        PG_RETURN_POINTER!(retval);
    }

    PG_RETURN_POINTER!(entry)
}

/* typedef struct { int32 *arrb; int32 *arre; } CHKVAL; */
#[repr(C)]
struct CHKVAL {
    arrb: *mut int32,
    arre: *mut int32,
}

/*
 * TS_execute callback for matching a tsquery operand to GIST leaf-page data.
 * Binary search of the sorted CRC array.  REAL.
 */
unsafe fn checkcondition_arr(
    checkval: *mut c_void,
    val: *mut QueryOperand,
    _data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let mut StopLow = (*(checkval as *mut CHKVAL)).arrb;
    let mut StopHigh = (*(checkval as *mut CHKVAL)).arre;

    /* Loop invariant: StopLow <= val < StopHigh */

    /* we are not able to find a prefix by hash value */
    if (*val).prefix {
        return TS_MAYBE;
    }

    while StopLow < StopHigh {
        let StopMiddle = StopLow.add(StopHigh.offset_from(StopLow) as usize / 2);
        if *StopMiddle == (*val).valcrc {
            return TS_MAYBE;
        } else if *StopMiddle < (*val).valcrc {
            StopLow = StopMiddle.add(1);
        } else {
            StopHigh = StopMiddle;
        }
    }

    TS_NO
}

/*
 * TS_execute callback for matching a tsquery operand to GIST non-leaf data.
 * Single signature-bit probe.  REAL.
 */
unsafe fn checkcondition_bit(
    checkval: *mut c_void,
    val: *mut QueryOperand,
    _data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let key = checkval as *mut SignTSVector;

    /* we are not able to find a prefix in signature tree */
    if (*val).prefix {
        return TS_MAYBE;
    }

    if GETBIT(GETSIGN(key), HASHVAL((*val).valcrc, GETSIGLEN(key))) != 0 {
        TS_MAYBE
    } else {
        TS_NO
    }
}

pub unsafe fn gtsvector_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let entry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY;
    let query: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 1);

    /* StrategyNumber strategy = PG_GETARG_UINT16(2); */
    /* Oid subtype = PG_GETARG_OID(3); */
    let recheck = PG_GETARG_POINTER!(fcinfo, 4) as *mut bool;
    let key = DatumGetPointer((*entry).key) as *mut SignTSVector;

    /* All cases served by this function are inexact */
    *recheck = true;

    if (*query).size == 0 {
        PG_RETURN_BOOL!(false);
    }

    if ISSIGNKEY(key) {
        if ISALLTRUE(key) {
            PG_RETURN_BOOL!(true);
        }

        PG_RETURN_BOOL!(TS_execute(
            GETQUERY(query),
            key as *mut c_void,
            TS_EXEC_PHRASE_NO_POS,
            checkcondition_bit,
        ))
    } else {
        /* only leaf pages */
        let mut chkval = CHKVAL {
            arrb: GETARR(key),
            arre: null_mut(),
        };
        chkval.arre = chkval.arrb.add(ARRNELEM(key) as usize);
        PG_RETURN_BOOL!(TS_execute(
            GETQUERY(query),
            &mut chkval as *mut CHKVAL as *mut c_void,
            TS_EXEC_PHRASE_NO_POS,
            checkcondition_arr,
        ))
    }
}

/*
 * static int32 unionkey(BITVECP sbase, SignTSVector *add, int siglen)
 * OR a SIGNKEY child's bits into sbase, or HASH an ARRKEY child's entries.
 * Returns 1 if `add` is ALLISTRUE (caller short-circuits).  REAL.
 */
unsafe fn unionkey(sbase: BITVECP, add: *mut SignTSVector, siglen: c_int) -> int32 {
    if ISSIGNKEY(add) {
        let sadd = GETSIGN(add);

        if ISALLTRUE(add) {
            return 1;
        }

        Assert!(GETSIGLEN(add) == siglen);

        for i in 0..siglen {
            let p = sbase.add(i as usize);
            *p = (*p as u8 | *sadd.add(i as usize) as u8) as c_char;
        }
    } else {
        let ptr = GETARR(add);

        for i in 0..ARRNELEM(add) {
            HASH(sbase, *ptr.add(i as usize), siglen);
        }
    }
    0
}

pub unsafe fn gtsvector_union(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let size = PG_GETARG_POINTER!(fcinfo, 1) as *mut c_int;
    let siglen = GET_SIGLEN();
    let result = gtsvector_alloc(SIGNKEY, siglen, null_mut());
    let base = GETSIGN(result);

    core::ptr::write_bytes(base, 0, siglen as usize);

    for i in 0..(*entryvec).n {
        if unionkey(base, GETENTRY(entryvec, i as usize), siglen) != 0 {
            (*result).flag |= ALLISTRUE;
            crate::varatt::SET_VARSIZE(
                result as *mut c_char,
                CALCGTSIZE((*result).flag, siglen) as int32,
            );
            break;
        }
    }

    *size = crate::varatt::VARSIZE(result as *const c_char) as c_int;

    PG_RETURN_POINTER!(result)
}

pub unsafe fn gtsvector_same(fcinfo: FunctionCallInfo) -> Datum {
    let a = PG_GETARG_POINTER!(fcinfo, 0) as *mut SignTSVector;
    let b = PG_GETARG_POINTER!(fcinfo, 1) as *mut SignTSVector;
    let result = PG_GETARG_POINTER!(fcinfo, 2) as *mut bool;
    let siglen = GET_SIGLEN();

    if ISSIGNKEY(a) {
        /* then b also ISSIGNKEY */
        if ISALLTRUE(a) && ISALLTRUE(b) {
            *result = true;
        } else if ISALLTRUE(a) {
            *result = false;
        } else if ISALLTRUE(b) {
            *result = false;
        } else {
            let sa = GETSIGN(a);
            let sb = GETSIGN(b);

            Assert!(GETSIGLEN(a) == siglen && GETSIGLEN(b) == siglen);

            *result = true;
            for i in 0..siglen {
                if *sa.add(i as usize) != *sb.add(i as usize) {
                    *result = false;
                    break;
                }
            }
        }
    } else {
        /* a and b ISARRKEY */
        let lena = ARRNELEM(a);
        let lenb = ARRNELEM(b);

        if lena != lenb {
            *result = false;
        } else {
            let ptra = GETARR(a);
            let ptrb = GETARR(b);

            *result = true;
            for i in 0..lena {
                if *ptra.add(i as usize) != *ptrb.add(i as usize) {
                    *result = false;
                    break;
                }
            }
        }
    }

    PG_RETURN_POINTER!(result)
}

/* static int32 sizebitvec(BITVECP sign, int siglen) -> pg_popcount. */
unsafe fn sizebitvec(sign: BITVECP, siglen: c_int) -> int32 {
    pg_popcount(sign, siglen) as int32
}

/* static int hemdistsign(BITVECP a, BITVECP b, int siglen) -- Hamming distance. */
unsafe fn hemdistsign(a: BITVECP, b: BITVECP, siglen: c_int) -> c_int {
    let mut dist = 0;

    for i in 0..siglen {
        let diff = (*a.add(i as usize) as u8) ^ (*b.add(i as usize) as u8);
        /* Using the popcount functions here isn't likely to win */
        dist += pg_number_of_ones[diff as usize] as c_int;
    }
    dist
}

/* static int hemdist(SignTSVector *a, SignTSVector *b) */
unsafe fn hemdist(a: *mut SignTSVector, b: *mut SignTSVector) -> c_int {
    let siglena = GETSIGLEN(a);
    let siglenb = GETSIGLEN(b);

    if ISALLTRUE(a) {
        if ISALLTRUE(b) {
            return 0;
        } else {
            return SIGLENBIT(siglenb) - sizebitvec(GETSIGN(b), siglenb);
        }
    } else if ISALLTRUE(b) {
        return SIGLENBIT(siglena) - sizebitvec(GETSIGN(a), siglena);
    }

    Assert!(siglena == siglenb);

    hemdistsign(GETSIGN(a), GETSIGN(b), siglena)
}

pub unsafe fn gtsvector_penalty(fcinfo: FunctionCallInfo) -> Datum {
    let origentry = PG_GETARG_POINTER!(fcinfo, 0) as *mut GISTENTRY; /* always ISSIGNKEY */
    let newentry = PG_GETARG_POINTER!(fcinfo, 1) as *mut GISTENTRY;
    let penalty = PG_GETARG_POINTER!(fcinfo, 2) as *mut f32;
    let siglen = GET_SIGLEN();
    let origval = DatumGetPointer((*origentry).key) as *mut SignTSVector;
    let newval = DatumGetPointer((*newentry).key) as *mut SignTSVector;
    let orig = GETSIGN(origval);

    *penalty = 0.0;

    if ISARRKEY(newval) {
        let sign = palloc(siglen as Size) as BITVECP;

        makesign(sign, newval, siglen);

        if ISALLTRUE(origval) {
            let siglenbit = SIGLENBIT(siglen);

            *penalty = (siglenbit - sizebitvec(sign, siglen)) as f32 / (siglenbit + 1) as f32;
        } else {
            *penalty = hemdistsign(sign, orig, siglen) as f32;
        }

        pfree(sign as *mut c_void);
    } else {
        *penalty = hemdist(origval, newval) as f32;
    }
    PG_RETURN_POINTER!(penalty)
}

/* typedef struct { bool allistrue; BITVECP sign; } CACHESIGN; */
struct CACHESIGN {
    allistrue: bool,
    sign: BITVECP,
}

/* static void fillcache(CACHESIGN *item, SignTSVector *key, int siglen) */
unsafe fn fillcache(item: *mut CACHESIGN, key: *mut SignTSVector, siglen: c_int) {
    (*item).allistrue = false;
    if ISARRKEY(key) {
        makesign((*item).sign, key, siglen);
    } else if ISALLTRUE(key) {
        (*item).allistrue = true;
    } else {
        core::ptr::copy_nonoverlapping(GETSIGN(key), (*item).sign, siglen as usize);
    }
}

/* #define WISH_F(a,b,c) (double)( -(double)(((a)-(b))*((a)-(b))*((a)-(b)))*(c) ) */
#[inline]
fn WISH_F(a: c_int, b: c_int, c: f64) -> f64 {
    let d = (a - b) as f64;
    -(d * d * d) * c
}

/* typedef struct { OffsetNumber pos; int32 cost; } SPLITCOST; */
#[derive(Clone, Copy)]
struct SPLITCOST {
    pos: OffsetNumber,
    cost: int32,
}

/* static int comparecost(const void *va, const void *vb) -> pg_cmp_s32(cost). */
fn comparecost(a: &SPLITCOST, b: &SPLITCOST) -> c_int {
    pg_cmp_s32(a.cost, b.cost)
}

/* static int hemdistcache(CACHESIGN *a, CACHESIGN *b, int siglen) */
unsafe fn hemdistcache(a: *const CACHESIGN, b: *const CACHESIGN, siglen: c_int) -> c_int {
    if (*a).allistrue {
        if (*b).allistrue {
            return 0;
        } else {
            return SIGLENBIT(siglen) - sizebitvec((*b).sign, siglen);
        }
    } else if (*b).allistrue {
        return SIGLENBIT(siglen) - sizebitvec((*a).sign, siglen);
    }

    hemdistsign((*a).sign, (*b).sign, siglen)
}

pub unsafe fn gtsvector_picksplit(fcinfo: FunctionCallInfo) -> Datum {
    let entryvec = PG_GETARG_POINTER!(fcinfo, 0) as *mut GistEntryVector;
    let v = PG_GETARG_POINTER!(fcinfo, 1) as *mut GIST_SPLITVEC;
    let siglen = GET_SIGLEN();
    let mut k: OffsetNumber;
    let mut j: OffsetNumber;
    let datum_l: *mut SignTSVector;
    let datum_r: *mut SignTSVector;
    let union_l: BITVECP;
    let union_r: BITVECP;
    let mut size_alpha: int32;
    let mut size_beta: int32;
    let mut size_waste: int32;
    let mut waste: int32 = -1;
    let nbytes: int32;
    let mut seed_1: OffsetNumber = 0;
    let mut seed_2: OffsetNumber = 0;
    let mut maxoff: OffsetNumber;
    let mut ptr: BITVECP;

    maxoff = ((*entryvec).n - 2) as OffsetNumber;
    nbytes = ((maxoff as int32) + 2) * core::mem::size_of::<OffsetNumber>() as int32;
    (*v).spl_left = palloc(nbytes as Size) as *mut OffsetNumber;
    (*v).spl_right = palloc(nbytes as Size) as *mut OffsetNumber;

    /* cache[] + a backing sign buffer for each entry (maxoff+2 entries). */
    let ncache = (maxoff as usize) + 2;
    let cache =
        palloc(core::mem::size_of::<CACHESIGN>() * ncache) as *mut CACHESIGN;
    let cache_sign = palloc((siglen as usize) * ncache) as *mut c_char;

    for jj in 0..ncache {
        (*cache.add(jj)).sign = cache_sign.add(siglen as usize * jj);
    }

    fillcache(
        cache.add(FirstOffsetNumber as usize),
        GETENTRY(entryvec, FirstOffsetNumber as usize),
        siglen,
    );

    k = FirstOffsetNumber;
    while k < maxoff {
        j = OffsetNumberNext(k);
        while j <= maxoff {
            if k == FirstOffsetNumber {
                fillcache(cache.add(j as usize), GETENTRY(entryvec, j as usize), siglen);
            }

            size_waste = hemdistcache(cache.add(j as usize), cache.add(k as usize), siglen);
            if size_waste > waste {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
            j = OffsetNumberNext(j);
        }
        k = OffsetNumberNext(k);
    }

    let mut left = (*v).spl_left;
    (*v).spl_nleft = 0;
    let mut right = (*v).spl_right;
    (*v).spl_nright = 0;

    if seed_1 == 0 || seed_2 == 0 {
        seed_1 = 1;
        seed_2 = 2;
    }

    /* form initial .. */
    datum_l = gtsvector_alloc(
        SIGNKEY | if (*cache.add(seed_1 as usize)).allistrue { ALLISTRUE } else { 0 },
        siglen,
        (*cache.add(seed_1 as usize)).sign,
    );
    datum_r = gtsvector_alloc(
        SIGNKEY | if (*cache.add(seed_2 as usize)).allistrue { ALLISTRUE } else { 0 },
        siglen,
        (*cache.add(seed_2 as usize)).sign,
    );
    union_l = GETSIGN(datum_l);
    union_r = GETSIGN(datum_r);
    maxoff = OffsetNumberNext(maxoff);
    fillcache(
        cache.add(maxoff as usize),
        GETENTRY(entryvec, maxoff as usize),
        siglen,
    );
    /* sort before ... */
    let mut costvector: Vec<SPLITCOST> = Vec::with_capacity(maxoff as usize);
    j = FirstOffsetNumber;
    while j <= maxoff {
        size_alpha = hemdistcache(cache.add(seed_1 as usize), cache.add(j as usize), siglen);
        size_beta = hemdistcache(cache.add(seed_2 as usize), cache.add(j as usize), siglen);
        costvector.push(SPLITCOST {
            pos: j,
            cost: (size_alpha - size_beta).abs(),
        });
        j = OffsetNumberNext(j);
    }
    costvector.sort_by(|a, b| comparecost(a, b).cmp(&0));

    for kk in 0..(maxoff as usize) {
        j = costvector[kk].pos;
        if j == seed_1 {
            *left = j;
            left = left.add(1);
            (*v).spl_nleft += 1;
            continue;
        } else if j == seed_2 {
            *right = j;
            right = right.add(1);
            (*v).spl_nright += 1;
            continue;
        }

        let cj = cache.add(j as usize);

        if ISALLTRUE(datum_l) || (*cj).allistrue {
            if ISALLTRUE(datum_l) && (*cj).allistrue {
                size_alpha = 0;
            } else {
                size_alpha = SIGLENBIT(siglen)
                    - sizebitvec(
                        if (*cj).allistrue { GETSIGN(datum_l) } else { (*cj).sign },
                        siglen,
                    );
            }
        } else {
            size_alpha = hemdistsign((*cj).sign, GETSIGN(datum_l), siglen);
        }

        if ISALLTRUE(datum_r) || (*cj).allistrue {
            if ISALLTRUE(datum_r) && (*cj).allistrue {
                size_beta = 0;
            } else {
                size_beta = SIGLENBIT(siglen)
                    - sizebitvec(
                        if (*cj).allistrue { GETSIGN(datum_r) } else { (*cj).sign },
                        siglen,
                    );
            }
        } else {
            size_beta = hemdistsign((*cj).sign, GETSIGN(datum_r), siglen);
        }

        if (size_alpha as f64)
            < (size_beta as f64) + WISH_F((*v).spl_nleft, (*v).spl_nright, 0.1)
        {
            if ISALLTRUE(datum_l) || (*cj).allistrue {
                if !ISALLTRUE(datum_l) {
                    core::ptr::write_bytes(GETSIGN(datum_l), 0xff, siglen as usize);
                }
            } else {
                ptr = (*cj).sign;
                for i in 0..siglen {
                    let p = union_l.add(i as usize);
                    *p = (*p as u8 | *ptr.add(i as usize) as u8) as c_char;
                }
            }
            *left = j;
            left = left.add(1);
            (*v).spl_nleft += 1;
        } else {
            if ISALLTRUE(datum_r) || (*cj).allistrue {
                if !ISALLTRUE(datum_r) {
                    core::ptr::write_bytes(GETSIGN(datum_r), 0xff, siglen as usize);
                }
            } else {
                ptr = (*cj).sign;
                for i in 0..siglen {
                    let p = union_r.add(i as usize);
                    *p = (*p as u8 | *ptr.add(i as usize) as u8) as c_char;
                }
            }
            *right = j;
            right = right.add(1);
            (*v).spl_nright += 1;
        }
    }

    *right = FirstOffsetNumber;
    *left = FirstOffsetNumber;
    (*v).spl_ldatum = PointerGetDatum(datum_l as *const c_void);
    (*v).spl_rdatum = PointerGetDatum(datum_r as *const c_void);

    PG_RETURN_POINTER!(v)
}

/*
 * Compatibility shim for pre-9.6 contrib/tsearch2 opclass declarations.
 */
pub unsafe fn gtsvector_consistent_oldsig(fcinfo: FunctionCallInfo) -> Datum {
    gtsvector_consistent(fcinfo)
}

pub unsafe fn gtsvector_options(_fcinfo: FunctionCallInfo) -> Datum {
    /*
     * local_relopts *relopts = (local_relopts *) PG_GETARG_POINTER(0);
     * init_local_reloptions(relopts, sizeof(GistTsVectorOptions));
     * add_local_int_reloption(relopts, "siglen", "signature length",
     *                         SIGLEN_DEFAULT, 1, SIGLEN_MAX,
     *                         offsetof(GistTsVectorOptions, siglen));
     */
    // TODO(pg-port): access/reloptions.h (local_relopts / init_local_reloptions /
    // add_local_int_reloption) is not yet ported.
    unimplemented!("gtsvector_options requires the unported reloptions framework")
}

// ================================================================
//   Tests -- self-contained signature-bit math.
// ================================================================

#[cfg(test)]
mod tests {
    use super::*;

    const SL: c_int = 16; // 16-byte signature == 128 bits, for hand-built tests.

    /* Allocate a zeroed bit-vector of `siglen` bytes. */
    unsafe fn newsign(siglen: c_int) -> BITVECP {
        let p = palloc(siglen as Size) as BITVECP;
        core::ptr::write_bytes(p, 0, siglen as usize);
        p
    }

    #[test]
    fn test_setbit_getbit_roundtrip() {
        unsafe {
            let s = newsign(SL);
            // Set a handful of bits across byte boundaries, then read them back.
            let bits = [0, 1, 7, 8, 15, 63, 64, 127];
            for &b in &bits {
                SETBIT(s, b);
            }
            for i in 0..SIGLENBIT(SL) {
                let want = bits.contains(&i);
                assert_eq!(GETBIT(s, i) != 0, want, "bit {i}");
            }
            // sizebitvec == popcount == number of distinct bits set.
            assert_eq!(sizebitvec(s, SL), bits.len() as int32);
        }
    }

    #[test]
    fn test_hashval_in_range_and_present() {
        unsafe {
            let s = newsign(SL);
            // A value HASHed into the signature is reported present at HASHVAL.
            let vals: [int32; 5] = [1, 12345, -42, 0x7fff_ffff, i32::MIN];
            for &v in &vals {
                HASH(s, v, SL);
            }
            for &v in &vals {
                let h = HASHVAL(v, SL);
                assert!(h >= 0 && h < SIGLENBIT(SL));
                assert_eq!(GETBIT(s, h), 1, "value {v} -> bit {h} must be set");
            }
        }
    }

    #[test]
    fn test_makesign_idempotent() {
        // makesign over the same array twice yields identical bits.
        unsafe {
            // Build an ARRKEY SignTSVector with a few CRC-ish int32 values.
            let vals: [int32; 6] = [10, 20, 30, 40, 50, 60];
            let a = gtsvector_alloc(ARRKEY, vals.len() as c_int, null_mut());
            let arr = GETARR(a);
            for (i, &v) in vals.iter().enumerate() {
                *arr.add(i) = v;
            }

            let s1 = newsign(SL);
            let s2 = newsign(SL);
            makesign(s1, a, SL);
            makesign(s2, a, SL);
            for i in 0..SL {
                assert_eq!(*s1.add(i as usize), *s2.add(i as usize), "byte {i}");
            }
            // And every value is present in the produced signature.
            for &v in &vals {
                assert_eq!(GETBIT(s1, HASHVAL(v, SL)), 1);
            }
        }
    }

    #[test]
    fn test_unionkey_is_bitwise_or_of_sigs() {
        // union of two SIGNKEY children == bitwise OR of their signatures.
        unsafe {
            let a = gtsvector_alloc(SIGNKEY, SL, null_mut());
            let b = gtsvector_alloc(SIGNKEY, SL, null_mut());
            let sa = GETSIGN(a);
            let sb = GETSIGN(b);
            core::ptr::write_bytes(sa, 0, SL as usize);
            core::ptr::write_bytes(sb, 0, SL as usize);
            SETBIT(sa, 3);
            SETBIT(sa, 40);
            SETBIT(sb, 40);
            SETBIT(sb, 70);

            let base = newsign(SL);
            assert_eq!(unionkey(base, a, SL), 0);
            assert_eq!(unionkey(base, b, SL), 0);

            // base must equal sa | sb, bit for bit.
            for i in 0..SIGLENBIT(SL) {
                let want = (GETBIT(sa, i) != 0) || (GETBIT(sb, i) != 0);
                assert_eq!(GETBIT(base, i) != 0, want, "bit {i}");
            }
            assert_eq!(sizebitvec(base, SL), 3); // bits 3,40,70
        }
    }

    #[test]
    fn test_unionkey_alltrue_short_circuits() {
        unsafe {
            let allt = gtsvector_alloc(SIGNKEY | ALLISTRUE, SL, null_mut());
            let base = newsign(SL);
            assert_eq!(unionkey(base, allt, SL), 1);
        }
    }

    #[test]
    fn test_hemdistsign_is_popcount_of_xor() {
        unsafe {
            let a = newsign(SL);
            let b = newsign(SL);
            SETBIT(a, 0);
            SETBIT(a, 1);
            SETBIT(a, 2);
            SETBIT(a, 3); // a has 4 bits in byte 0
            SETBIT(b, 4);
            SETBIT(b, 5);
            SETBIT(b, 6);
            SETBIT(b, 7); // b has 4 disjoint bits in byte 0
            // XOR has all 8 bits in byte 0 set -> distance 8.
            assert_eq!(hemdistsign(a, b, SL), 8);
            assert_eq!(hemdistsign(a, a, SL), 0);
        }
    }

    #[test]
    fn test_same_agrees_with_union_on_signkeys() {
        // If union(a,b) == a (i.e. b's bits subset of a), and we compare a to the
        // union, gtsvector_same's per-byte equality must report true.
        unsafe {
            let a = gtsvector_alloc(SIGNKEY, SL, null_mut());
            let b = gtsvector_alloc(SIGNKEY, SL, null_mut());
            core::ptr::write_bytes(GETSIGN(a), 0, SL as usize);
            core::ptr::write_bytes(GETSIGN(b), 0, SL as usize);
            // Identical signatures.
            SETBIT(GETSIGN(a), 11);
            SETBIT(GETSIGN(a), 99);
            SETBIT(GETSIGN(b), 11);
            SETBIT(GETSIGN(b), 99);

            // gtsvector_same per-byte loop, replicated directly (no fcinfo).
            let sa = GETSIGN(a);
            let sb = GETSIGN(b);
            let mut eq = true;
            for i in 0..SL {
                if *sa.add(i as usize) != *sb.add(i as usize) {
                    eq = false;
                    break;
                }
            }
            assert!(eq);

            // Flip one bit in b: now they differ.
            SETBIT(GETSIGN(b), 50);
            let mut eq2 = true;
            for i in 0..SL {
                if *sa.add(i as usize) != *sb.add(i as usize) {
                    eq2 = false;
                    break;
                }
            }
            assert!(!eq2);
        }
    }

    #[test]
    fn test_alltrue_compress_detection() {
        // An all-0xff signature is exactly the ALLISTRUE condition the compress
        // path looks for: every byte == 0xff.
        unsafe {
            let s = palloc(SL as Size) as BITVECP;
            core::ptr::write_bytes(s, 0xff, SL as usize);
            let mut all = true;
            for i in 0..SL {
                if (*s.add(i as usize) as u8) != 0xff {
                    all = false;
                }
            }
            assert!(all);
            // sizebitvec of all-true == every bit set.
            assert_eq!(sizebitvec(s, SL), SIGLENBIT(SL));
        }
    }

    #[test]
    fn test_wish_f_sign() {
        // WISH_F = -((a-b)^3)*c: positive when a<b, negative when a>b, 0 when equal.
        assert!(WISH_F(1, 3, 0.1) > 0.0);
        assert!(WISH_F(3, 1, 0.1) < 0.0);
        assert_eq!(WISH_F(2, 2, 0.1), 0.0);
    }
}
