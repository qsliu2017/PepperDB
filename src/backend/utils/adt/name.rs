//! Functions for the built-in type "name". Translated from
//! src/backend/utils/adt/name.c.
//!
//! `name` is the fixed-width catalog identifier type: a C string null-padded to
//! exactly `NAMEDATALEN` (64) bytes. Covers the I/O routines
//! (`namein`/`nameout`/`namesend`), the comparison suite
//! (`nameeq`..`namege`/`btnamecmp`), and the public helpers
//! (`namestrcpy`/`namestrcmp`/`nameconcatoid`).
//!
//! A `Name` (`*mut NameData`) is produced by leaking a boxed `NameData` (no
//! MemoryContext yet, like int.rs/varlena.rs output leaking). Comparisons use
//! the C-collation `strncmp(.., NAMEDATALEN)` fast path; other collations route
//! through `varstr_cmp`.

use crate::c::{NameData, NAMEDATALEN};
use crate::catalog::genbki::C_COLLATION_OID;
use crate::fmgr::{FunctionCallInfoBaseData, PG_GET_COLLATION};
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, Datum, DatumGetCString, DatumGetName, Int32GetDatum,
    NameGetDatum,
};
use crate::postgres_ext::Oid;

#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is a NUL-terminated C string
    // that outlives the call.
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}

#[inline]
fn pg_return_cstring(s: &str) -> Datum {
    let bytes: Vec<u8> = s.bytes().take_while(|&b| b != 0).collect();
    let c = std::ffi::CString::new(bytes).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

/// `PG_GETARG_NAME(n)`: borrow the argument as a `&NameData`.
///
/// SAFETY: the arg Datum is a valid `Name` pointer that outlives the call.
#[inline]
unsafe fn pg_getarg_name<'a>(fcinfo: &FunctionCallInfoBaseData, n: usize) -> &'a NameData {
    &*DatumGetName(fcinfo.args[n].value)
}

/// Build a leaked, zero-padded `NameData` from the first `len` bytes of `bytes`.
///
/// The result is carried in a `Name` (byref) Datum that must outlive the call,
/// so the owned `Box<NameData>` is leaked into a raw pointer -- the same
/// output-Datum convention int.rs (`pg_return_cstring` -> `CString::into_raw`)
/// uses. TODO(memory-context): reclaim via the per-call/statement memory context
/// when that lands, replacing the leak.
fn make_name(bytes: &[u8]) -> *mut NameData {
    let mut nd = Box::new(NameData { data: [0u8; NAMEDATALEN] });
    let n = bytes.len().min(NAMEDATALEN - 1);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    Box::into_raw(nd)
}

/// The NUL-terminated logical string within a `NameData` (`NameStr`).
fn name_str(nd: &NameData) -> &[u8] {
    let end = nd.data.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN);
    &nd.data[..end]
}

/// Number of bytes of `s` that fit in `NAMEDATALEN - 1`, clipped to a UTF-8
/// char boundary (the role of `pg_mbcliplen` for the default encoding).
fn name_cliplen(s: &[u8]) -> usize {
    let mut n = s.len().min(NAMEDATALEN - 1);
    while n > 0 && (s[n] & 0xC0) == 0x80 {
        n -= 1;
    }
    n
}

// ===========================================================================
//   USER I/O ROUTINES
// ===========================================================================

/// PG `namein`: converts a cstring to the internal `name` representation.
pub fn namein(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = pg_getarg_cstring(fcinfo, 0);
    let bytes = s.as_bytes();
    let len = if bytes.len() >= NAMEDATALEN {
        name_cliplen(bytes)
    } else {
        bytes.len()
    };
    let result = make_name(&bytes[..len]);
    // SAFETY: freshly leaked NameData we own.
    NameGetDatum(unsafe { &*result })
}

/// PG `nameout`: converts the internal representation to a cstring.
pub fn nameout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // SAFETY: arg is a valid Name.
    let s = unsafe { pg_getarg_name(fcinfo, 0) };
    let bytes = name_str(s);
    pg_return_cstring(&String::from_utf8_lossy(bytes))
}

/// PG `namerecv`: converts external binary format to name.
pub fn namerecv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("namerecv needs the binary wire StringInfo (pq_getmsgtext) marshalling")
}

/// PG `namesend`: converts name to binary format.
pub fn namesend(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("namesend needs pq_begintypsend/pq_endtypsend bytea boxing")
}

// ===========================================================================
//   COMPARISON / SORTING ROUTINES
// ===========================================================================

/// PG `namecmp`: compare two names under `collid` (-1/0/1).
fn namecmp(arg1: &NameData, arg2: &NameData, collid: Oid) -> i32 {
    if collid == C_COLLATION_OID {
        // Fast path used in system catalogs: strncmp over NAMEDATALEN. Since the
        // unused tail is zero-padded, comparing the logical strings is equivalent.
        let s1 = name_str(arg1);
        let s2 = name_str(arg2);
        return match s1.cmp(s2) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        };
    }
    // Else rely on the varstr infrastructure (C/default path is memcmp).
    crate::backend::utils::adt::varlena::varstr_cmp(name_str(arg1), name_str(arg2), collid)
}

macro_rules! name_cmp_op {
    ($name:ident, $op:tt) => {
        #[doc = concat!("PG `", stringify!($name), "`.")]
        pub fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            // SAFETY: both args are valid Names.
            let (a1, a2) = unsafe { (pg_getarg_name(fcinfo, 0), pg_getarg_name(fcinfo, 1)) };
            let result = namecmp(a1, a2, PG_GET_COLLATION(fcinfo)) $op 0;
            BoolGetDatum(result)
        }
    };
}

name_cmp_op!(nameeq, ==);
name_cmp_op!(namene, !=);
name_cmp_op!(namelt, <);
name_cmp_op!(namele, <=);
name_cmp_op!(namegt, >);
name_cmp_op!(namege, >=);

/// PG `btnamecmp`: btree 3-way comparison support for name.
pub fn btnamecmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // SAFETY: both args are valid Names.
    let (a1, a2) = unsafe { (pg_getarg_name(fcinfo, 0), pg_getarg_name(fcinfo, 1)) };
    Int32GetDatum(namecmp(a1, a2, PG_GET_COLLATION(fcinfo)))
}

/// PG `btnamesortsupport`: SortSupport setup for name.
pub fn btnamesortsupport(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("btnamesortsupport needs varstr_sortsupport (utils::varlena)")
}

// ===========================================================================
//   MISCELLANEOUS PUBLIC ROUTINES
// ===========================================================================

/// PG `namestrcpy`: copy a C string into a `Name`, zero-padding the rest. The
/// destination is always NUL-terminated within `NAMEDATALEN`.
pub fn namestrcpy(name: &mut NameData, s: &str) {
    name.data = [0u8; NAMEDATALEN];
    let bytes = s.as_bytes();
    let n = bytes.len().min(NAMEDATALEN - 1);
    name.data[..n].copy_from_slice(&bytes[..n]);
}

/// PG `namestrcmp`: compare a `Name` to a C string (assumes C collation).
/// Returns -1/0/1.
#[must_use]
pub fn namestrcmp(name: &NameData, s: &str) -> i32 {
    let a = name_str(name);
    // strncmp(.., NAMEDATALEN): the C string is also clipped logically.
    let b = &s.as_bytes()[..s.len().min(NAMEDATALEN)];
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// PG `nameconcatoid`: `name || '_' || oid`, truncating the name part (not the
/// suffix) so the result fits in `NAMEDATALEN`.
pub fn nameconcatoid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // SAFETY: arg 0 is a valid Name.
    let nam = unsafe { pg_getarg_name(fcinfo, 0) };
    let oid = crate::postgres::DatumGetObjectId(fcinfo.args[1].value);

    let suffix = format!("_{}", oid.0);
    let suflen = suffix.len();
    let name_bytes = name_str(nam);
    let mut namlen = name_bytes.len();

    if namlen + suflen >= NAMEDATALEN {
        // Truncate the name part to a char boundary leaving room for the suffix.
        let limit = NAMEDATALEN - 1 - suflen;
        let mut n = namlen.min(limit);
        while n > 0 && (name_bytes[n] & 0xC0) == 0x80 {
            n -= 1;
        }
        namlen = n;
    }

    let mut joined = Vec::with_capacity(namlen + suflen);
    joined.extend_from_slice(&name_bytes[..namlen]);
    joined.extend_from_slice(suffix.as_bytes());
    let result = make_name(&joined);
    // SAFETY: freshly leaked NameData we own.
    NameGetDatum(unsafe { &*result })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetBool, NullableDatum, ObjectIdGetDatum};
    use crate::postgres_ext::Oid;

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

    fn cstr_datum(s: &str) -> Datum {
        let c = std::ffi::CString::new(s).unwrap();
        CStringGetDatum(c.into_raw())
    }

    fn name_datum(s: &str) -> Datum {
        let result = make_name(s.as_bytes());
        // SAFETY: freshly leaked NameData we own.
        NameGetDatum(unsafe { &*result })
    }

    fn out_to_string(d: Datum) -> String {
        let p = DatumGetCString(d);
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }

    #[test]
    fn namein_nameout_roundtrip() {
        for s in ["", "a", "pg_class", "some_identifier_name"] {
            let mut inf = fc(&[cstr_datum(s)]);
            let d = namein(&mut inf);
            let mut of = fc(&[d]);
            assert_eq!(out_to_string(nameout(&mut of)), s, "{s}");
        }
    }

    #[test]
    fn namein_truncates_oversize() {
        let long = "x".repeat(100);
        let mut inf = fc(&[cstr_datum(&long)]);
        let d = namein(&mut inf);
        let mut of = fc(&[d]);
        let back = out_to_string(nameout(&mut of));
        // Truncated to NAMEDATALEN - 1 bytes.
        assert_eq!(back.len(), NAMEDATALEN - 1);
        assert!(long.starts_with(&back));
    }

    #[test]
    fn namein_zero_pads() {
        let result = make_name(b"ab");
        // SAFETY: freshly leaked NameData we own.
        let nd = unsafe { &*result };
        assert_eq!(&nd.data[..2], b"ab");
        assert!(nd.data[2..].iter().all(|&b| b == 0), "tail must be zero-padded");
    }

    #[test]
    fn name_comparisons() {
        let a = name_datum("abc");
        let a2 = name_datum("abc");
        let b = name_datum("abd");
        let pre = name_datum("ab");
        assert!(DatumGetBool(nameeq(&mut fc(&[a, a2]))));
        assert!(DatumGetBool(namene(&mut fc(&[a, b]))));
        assert!(DatumGetBool(namelt(&mut fc(&[pre, a]))));
        assert!(DatumGetBool(namelt(&mut fc(&[a, b]))));
        assert!(DatumGetBool(namele(&mut fc(&[a, a2]))));
        assert!(DatumGetBool(namegt(&mut fc(&[b, a]))));
        assert!(DatumGetBool(namege(&mut fc(&[a, a2]))));
        assert!(crate::postgres::DatumGetInt32(btnamecmp(&mut fc(&[a, b]))) < 0);
        assert_eq!(crate::postgres::DatumGetInt32(btnamecmp(&mut fc(&[a, a2]))), 0);
    }

    #[test]
    fn namestrcpy_and_namestrcmp() {
        let mut nd = NameData { data: [0u8; NAMEDATALEN] };
        namestrcpy(&mut nd, "hello");
        assert_eq!(&nd.data[..5], b"hello");
        assert_eq!(nd.data[5], 0);
        assert_eq!(namestrcmp(&nd, "hello"), 0);
        assert!(namestrcmp(&nd, "hellp") < 0);
        assert!(namestrcmp(&nd, "hell") > 0);
    }

    #[test]
    fn nameconcatoid_basic_and_truncate() {
        let r = nameconcatoid(&mut fc(&[name_datum("foo"), ObjectIdGetDatum(Oid(42))]));
        let mut of = fc(&[r]);
        assert_eq!(out_to_string(nameout(&mut of)), "foo_42");

        // Oversize: name truncated, suffix preserved.
        let long = name_datum(&"x".repeat(70));
        let r = nameconcatoid(&mut fc(&[long, ObjectIdGetDatum(Oid(7))]));
        let mut of = fc(&[r]);
        let back = out_to_string(nameout(&mut of));
        assert!(back.ends_with("_7"), "suffix kept: {back}");
        assert!(back.len() < NAMEDATALEN, "fits in NAMEDATALEN: {}", back.len());
    }

    #[test]
    fn fmgr_table_binds_namein() {
        use crate::utils::fmgrtab::fmgr_builtins;
        let entry = fmgr_builtins
            .iter()
            .find(|b| b.func_name == "namein")
            .expect("namein present");
        let func = entry.func.expect("namein bound");
        let mut inf = fc(&[cstr_datum("bound_name")]);
        let d = func(&mut inf);
        let mut of = fc(&[d]);
        assert_eq!(out_to_string(nameout(&mut of)), "bound_name");
    }
}
