//! Translated from PostgreSQL src/include/utils/builtins.h

use bitflags::bitflags;

use crate::c::{int2vector, oidvector, text};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::palloc::MemoryContext;

/// Sign + the most decimal digits an 8-byte number could have.
pub const MAXINT8LEN: usize = 20;

// bool.c -- success bool + out-param -> Option (body in backend/utils/adt/bool.rs).
pub use crate::backend::utils::adt::bool::{parse_bool, parse_bool_with_len};

// domains.c -- escontext / void**extra preserved as raw for now (TODO(panic)).
pub fn domain_check(
    _value: Datum,
    _isnull: bool,
    _domain_type: Oid,
    _extra: &mut *mut core::ffi::c_void,
    _mcxt: MemoryContext,
) {
    unimplemented!()
}
pub fn domain_check_safe(
    _value: Datum,
    _isnull: bool,
    _domain_type: Oid,
    _extra: &mut *mut core::ffi::c_void,
    _mcxt: MemoryContext,
    // node: &mut Node escontext
) -> bool {
    unimplemented!()
}
pub fn errdatatype(_datatype_oid: Oid) -> i32 {
    unimplemented!()
}
pub fn errdomainconstraint(_datatype_oid: Oid, _conname: &str) -> i32 {
    unimplemented!()
}

// encode.c
pub fn hex_encode(_src: &[u8], _dst: &mut [u8]) -> u64 {
    unimplemented!()
}
pub fn hex_decode(_src: &[u8], _dst: &mut [u8]) -> u64 {
    unimplemented!()
}
pub fn hex_decode_safe(_src: &[u8], _dst: &mut [u8]) -> u64 {
    unimplemented!()
}

// int.c
/// PG `buildint2vector`: build a 1-D `int2vector` from `int2s`. The int2 sibling
/// of [`buildoidvector`] (same array-header layout, elemtype INT2OID). The buffer
/// is leaked (no MemoryContext yet); callers carry it in a byref Datum.
/// TODO(memory-context): reclaim via the per-call context when palloc lands.
#[must_use]
#[allow(
    clippy::cast_ptr_alignment,
    reason = "the buffer is a fresh u64-backed Box (8-byte aligned), which satisfies \
              int2vector's 4-byte field alignment; the cast is sound"
)]
pub fn buildint2vector(int2s: &[i16]) -> *mut int2vector {
    use crate::varatt::SET_VARSIZE;
    let n = int2s.len();
    // Int2VectorSize(n): the fixed header up to `values`, plus n int16 elements.
    let header = core::mem::offset_of!(int2vector, values);
    let total = header + core::mem::size_of_val(int2s);
    let words = total.div_ceil(core::mem::size_of::<u64>());
    let mut buf = vec![0u64; words].into_boxed_slice();
    let base = buf.as_mut_ptr().cast::<u8>();
    let ptr = base.cast::<int2vector>();
    // SAFETY: `base`/`ptr` head a freshly-allocated, 8-byte-aligned buffer of at
    // least `total` bytes laid out exactly as `int2vector` (repr(C)); the field
    // writes and the element copy stay in bounds.
    unsafe {
        SET_VARSIZE(base, total as u32);
        (*ptr).ndim = 1;
        (*ptr).dataoffset = 0; // never any nulls
        (*ptr).elemtype = crate::catalog::genbki::INT2OID;
        (*ptr).dim1 = i32::try_from(n).unwrap_or(0);
        (*ptr).lbound1 = 0;
        if n > 0 {
            let vptr = std::ptr::addr_of_mut!((*ptr).values).cast::<i16>();
            core::ptr::copy_nonoverlapping(int2s.as_ptr(), vptr, n);
        }
    }
    Box::leak(buf).as_mut_ptr().cast::<int2vector>()
}

// name.c -- bodies in backend/utils/adt/name.rs (type-centric: &mut NameData).
pub use crate::backend::utils::adt::name::{namestrcmp, namestrcpy};

// numutils.c -- the _safe variants route errors via escontext (TODO(panic)/Result).
pub fn pg_strtoint16(_s: &str) -> i16 {
    unimplemented!()
}
pub fn pg_strtoint16_safe(_s: &str) -> i16 {
    unimplemented!()
}
pub fn pg_strtoint32(_s: &str) -> i32 {
    unimplemented!()
}
pub fn pg_strtoint32_safe(_s: &str) -> i32 {
    unimplemented!()
}
pub fn pg_strtoint64(_s: &str) -> i64 {
    unimplemented!()
}
pub fn pg_strtoint64_safe(_s: &str) -> i64 {
    unimplemented!()
}
pub fn uint32in_subr(_s: &str, _typname: &str) -> u32 {
    unimplemented!()
}
pub fn uint64in_subr(_s: &str, _typname: &str) -> u64 {
    unimplemented!()
}
pub fn pg_itoa(_i: i16, _a: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn pg_ultoa_n(_value: u32, _a: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn pg_ulltoa_n(_value: u64, _a: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn pg_ltoa(_value: i32, _a: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn pg_lltoa(_value: i64, _a: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn pg_ultostr_zeropad(_str: &mut [u8], _value: u32, _minwidth: i32) -> *mut u8 {
    unimplemented!()
}
pub fn pg_ultostr(_str: &mut [u8], _value: u32) -> *mut u8 {
    unimplemented!()
}

// oid.c
/// PG `buildoidvector`: build a 1-D `oidvector` from `oids`. Lays out the standard
/// array header (ndim 1, dataoffset 0, elemtype OID, lbound 0 for historical
/// reasons) followed by the element OIDs, with the varlena length set. The buffer
/// is leaked (no MemoryContext yet); callers carry it in a byref Datum.
/// TODO(memory-context): reclaim via the per-call context when palloc lands.
#[must_use]
#[allow(
    clippy::cast_ptr_alignment,
    reason = "the buffer is a fresh u64-backed Box (8-byte aligned), which satisfies \
              oidvector's 4-byte field alignment; the cast is sound"
)]
pub fn buildoidvector(oids: &[Oid]) -> *mut oidvector {
    use crate::varatt::SET_VARSIZE;
    let n = oids.len();
    // OidVectorSize(n): the fixed header up to `values`, plus n element OIDs.
    let header = core::mem::offset_of!(oidvector, values);
    let total = header + core::mem::size_of_val(oids);
    // Back the buffer with a `u64` Vec so it is 8-byte aligned (>= oidvector's
    // 4-byte field alignment); a plain `Vec<u8>` would not guarantee alignment.
    let words = total.div_ceil(core::mem::size_of::<u64>());
    let mut buf = vec![0u64; words].into_boxed_slice();
    let base = buf.as_mut_ptr().cast::<u8>();
    let ptr = base.cast::<oidvector>();
    // SAFETY: `base`/`ptr` head a freshly-allocated, 8-byte-aligned buffer of at
    // least `total` bytes laid out exactly as `oidvector` (repr(C)); the field
    // writes and the element copy stay in bounds.
    unsafe {
        SET_VARSIZE(base, total as u32);
        (*ptr).ndim = 1;
        (*ptr).dataoffset = 0;
        (*ptr).elemtype = crate::catalog::genbki::OIDOID;
        (*ptr).dim1 = i32::try_from(n).unwrap_or(0);
        (*ptr).lbound1 = 0;
        if n > 0 {
            let vptr = std::ptr::addr_of_mut!((*ptr).values).cast::<Oid>();
            core::ptr::copy_nonoverlapping(oids.as_ptr(), vptr, n);
        }
    }
    Box::leak(buf).as_mut_ptr().cast::<oidvector>()
}
pub fn check_valid_oidvector(_oid_array: &oidvector) {
    unimplemented!()
}
pub fn oidparse(/* node: &Node */) -> Oid {
    unimplemented!()
}
pub fn oid_cmp(_p1: Oid, _p2: Oid) -> core::cmp::Ordering {
    unimplemented!()
}

// regexp.c -- out-param exact -> tuple.
pub fn regexp_fixed_prefix(
    _text_re: &text,
    _case_insensitive: bool,
    _collation: Oid,
) -> (Option<String>, bool) {
    unimplemented!()
}

// ruleutils.c
pub static mut quote_all_identifiers: bool = false;
pub fn quote_identifier(_ident: &str) -> &str {
    unimplemented!()
}
pub fn quote_qualified_identifier(_qualifier: &str, _ident: &str) -> String {
    unimplemented!()
}
pub fn generate_operator_clause(
    // buf: &mut StringInfo
    _leftop: &str,
    _leftoptype: Oid,
    _opoid: Oid,
    _rightop: &str,
    _rightoptype: Oid,
) {
    unimplemented!()
}

// varchar.c
pub fn bpchartruelen(_s: &mut [u8], _len: i32) -> i32 {
    unimplemented!()
}

// varlena.c -- bodies in backend/utils/adt/varlena.rs.
pub use crate::backend::utils::adt::varlena::{
    cstring_to_text, cstring_to_text_with_len, text_to_cstring, text_to_cstring_buffer,
};

/// CStringGetTextDatum(s) -> PointerGetDatum(cstring_to_text(s)).
pub fn CStringGetTextDatum(s: &str) -> Datum {
    Datum(cstring_to_text(s) as usize)
}
/// TextDatumGetCString(d) -> text_to_cstring(d as text*).
pub fn TextDatumGetCString(d: Datum) -> String {
    text_to_cstring(unsafe { &*(d.0 as *const text) })
}

// xid.c -- comparators over Datum-stored xids.
pub fn xidComparator(_arg1: &u32, _arg2: &u32) -> core::cmp::Ordering {
    unimplemented!()
}
pub fn xidLogicalComparator(_arg1: &u32, _arg2: &u32) -> core::cmp::Ordering {
    unimplemented!()
}

// inet_cidr_ntop.c
pub fn pg_inet_cidr_ntop(_af: i32, _src: &[u8], _bits: i32, _dst: &mut [u8]) -> Option<String> {
    unimplemented!()
}

// inet_net_pton.c -- returns # of bits or -1.
pub fn pg_inet_net_pton(_af: i32, _src: &str, _dst: &mut [u8]) -> i32 {
    unimplemented!()
}

// network.c -- out-param failure -> Option.
pub fn convert_network_to_scalar(_value: Datum, _typid: Oid) -> Option<f64> {
    unimplemented!()
}
pub fn network_scan_first(_in: Datum) -> Datum {
    unimplemented!()
}
pub fn network_scan_last(_in: Datum) -> Datum {
    unimplemented!()
}
pub fn clean_ipv6_addr(_addr_family: i32, _addr: &mut [u8]) {
    unimplemented!()
}

// numeric.c -- fmgr-callable, keeps fcinfo signature.
pub fn numeric_float8_no_overflow(/* fcinfo */) -> Datum {
    unimplemented!()
}

// format_type.c
bitflags! {
    /// Control flags for format_type_extended.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FormatType: u16 {
        const TYPEMOD_GIVEN = 0x01; // typemod defined by caller
        const ALLOW_INVALID = 0x02; // allow invalid types
        const FORCE_QUALIFY = 0x04; // force qualification of type
        const INVALID_AS_NULL = 0x08; // NULL if undefined
    }
}

pub fn format_type_extended(_type_oid: Oid, _typemod: i32, _flags: FormatType) -> Option<String> {
    unimplemented!()
}
pub fn format_type_be(_type_oid: Oid) -> String {
    unimplemented!()
}
pub fn format_type_be_qualified(_type_oid: Oid) -> String {
    unimplemented!()
}
pub fn format_type_with_typemod(_type_oid: Oid, _typemod: i32) -> String {
    unimplemented!()
}
pub fn type_maximum_size(_type_oid: Oid, _typemod: i32) -> i32 {
    unimplemented!()
}

// quote.c
pub fn quote_literal_cstr(_rawstr: &str) -> String {
    unimplemented!()
}
