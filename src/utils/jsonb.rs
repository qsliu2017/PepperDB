//! Translated from PostgreSQL src/include/utils/jsonb.h

use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::numeric::Numeric;

/// Tokens used when sequentially processing a jsonb value.
pub enum JsonbIteratorToken {
    Done,
    Key,
    Value,
    Elem,
    BeginArray,
    EndArray,
    BeginObject,
    EndObject,
}

/* Strategy numbers for GIN index opclasses */
pub const JsonbContainsStrategyNumber: u16 = 7;
pub const JsonbExistsStrategyNumber: u16 = 9;
pub const JsonbExistsAnyStrategyNumber: u16 = 10;
pub const JsonbExistsAllStrategyNumber: u16 = 11;
pub const JsonbJsonpathExistsStrategyNumber: u16 = 15;
pub const JsonbJsonpathPredicateStrategyNumber: u16 = 16;

// jsonb_ops GIN entry prefix byte: type ordinals (0x01-0x05) + a HASHED flag.
// Per bitflags appendix D this is not a flag set; keep as plain consts.
pub const JGINFLAG_KEY: u8 = 0x01; // key (or string array element)
pub const JGINFLAG_NULL: u8 = 0x02; // null value
pub const JGINFLAG_BOOL: u8 = 0x03; // boolean value
pub const JGINFLAG_NUM: u8 = 0x04; // numeric value
pub const JGINFLAG_STR: u8 = 0x05; // string value (if not an array element)
pub const JGINFLAG_HASHED: u8 = 0x10; // OR'd in if value was hashed
pub const JGIN_MAXLENGTH: i32 = 125; // max length of text part before hashing

/// JEntry: a packed on-disk word. Low 28 bits = data length or end+1 offset, next
/// 3 bits = type, high bit = whether the low bits store an offset. Per bitflags
/// appendix C this packs a number beside flags, so keep the raw word + accessors.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct JEntry(pub u32);

pub const JENTRY_OFFLENMASK: u32 = 0x0FFFFFFF;
pub const JENTRY_TYPEMASK: u32 = 0x70000000;
pub const JENTRY_HAS_OFF: u32 = 0x80000000;

/* values stored in the type bits */
pub const JENTRY_ISSTRING: u32 = 0x00000000;
pub const JENTRY_ISNUMERIC: u32 = 0x10000000;
pub const JENTRY_ISBOOL_FALSE: u32 = 0x20000000;
pub const JENTRY_ISBOOL_TRUE: u32 = 0x30000000;
pub const JENTRY_ISNULL: u32 = 0x40000000;
pub const JENTRY_ISCONTAINER: u32 = 0x50000000; // array or object

impl JEntry {
    /// JBE_OFFLENFLD
    pub const fn offlenfld(self) -> u32 {
        self.0 & JENTRY_OFFLENMASK
    }
    /// JBE_HAS_OFF
    pub const fn has_off(self) -> bool {
        (self.0 & JENTRY_HAS_OFF) != 0
    }
    /// JBE_ISSTRING
    pub const fn is_string(self) -> bool {
        (self.0 & JENTRY_TYPEMASK) == JENTRY_ISSTRING
    }
    /// JBE_ISNUMERIC
    pub const fn is_numeric(self) -> bool {
        (self.0 & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC
    }
    /// JBE_ISCONTAINER
    pub const fn is_container(self) -> bool {
        (self.0 & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER
    }
    /// JBE_ISNULL
    pub const fn is_null(self) -> bool {
        (self.0 & JENTRY_TYPEMASK) == JENTRY_ISNULL
    }
    /// JBE_ISBOOL_TRUE
    pub const fn is_bool_true(self) -> bool {
        (self.0 & JENTRY_TYPEMASK) == JENTRY_ISBOOL_TRUE
    }
    /// JBE_ISBOOL_FALSE
    pub const fn is_bool_false(self) -> bool {
        (self.0 & JENTRY_TYPEMASK) == JENTRY_ISBOOL_FALSE
    }
    /// JBE_ISBOOL
    pub const fn is_bool(self) -> bool {
        self.is_bool_true() || self.is_bool_false()
    }
}

/// JBE_ADVANCE_OFFSET: advance `offset` to the next JEntry.
pub fn jbe_advance_offset(offset: &mut u32, je: JEntry) {
    if je.has_off() {
        *offset = je.offlenfld();
    } else {
        *offset += je.offlenfld();
    }
}

/// We store an offset (not a length) every JB_OFFSET_STRIDE children.
pub const JB_OFFSET_STRIDE: i32 = 32;

/// A jsonb array or object node within a Jsonb Datum. On-disk: a header word
/// followed by a JEntry FAM, then each child's variable-length data.
#[repr(C)]
pub struct JsonbContainer {
    /// number of elements or key/value pairs, and flags
    pub header: u32,
    // JEntry children[FLEXIBLE_ARRAY_MEMBER]; access via children().
}

/* flags for the header field in JsonbContainer */
pub const JB_CMASK: u32 = 0x0FFFFFFF; // mask for count field
pub const JB_FSCALAR: u32 = 0x10000000; // flag bits
pub const JB_FOBJECT: u32 = 0x20000000;
pub const JB_FARRAY: u32 = 0x40000000;

impl JsonbContainer {
    /// JsonContainerSize
    pub const fn size(&self) -> u32 {
        self.header & JB_CMASK
    }
    /// JsonContainerIsScalar
    pub const fn is_scalar(&self) -> bool {
        (self.header & JB_FSCALAR) != 0
    }
    /// JsonContainerIsObject
    pub const fn is_object(&self) -> bool {
        (self.header & JB_FOBJECT) != 0
    }
    /// JsonContainerIsArray
    pub const fn is_array(&self) -> bool {
        (self.header & JB_FARRAY) != 0
    }

    /// The trailing JEntry array. Length is the count field (size()).
    /// SAFETY: `self` must point into a JsonbContainer buffer of its recorded size.
    pub fn children(&self) -> &[JEntry] {
        let n = self.size() as usize;
        unsafe {
            let base = std::ptr::from_ref::<Self>(self).add(1).cast::<JEntry>();
            core::slice::from_raw_parts(base, n)
        }
    }
}

/// The top-level on-disk format for a jsonb datum (a varlena).
#[repr(C)]
pub struct Jsonb {
    pub vl_len_: i32, // varlena header (do not touch directly; use VARSIZE/SET_VARSIZE)
    pub root: JsonbContainer,
}

/// JB_ROOT_COUNT / IS_SCALAR / IS_OBJECT / IS_ARRAY operate on the root header,
/// which begins at VARDATA(jbp); here equal to `root.header` for a detoasted Jsonb.
impl Jsonb {
    pub fn root_count(&self) -> u32 {
        self.root.header & JB_CMASK
    }
    pub fn root_is_scalar(&self) -> bool {
        (self.root.header & JB_FSCALAR) != 0
    }
    pub fn root_is_object(&self) -> bool {
        (self.root.header & JB_FOBJECT) != 0
    }
    pub fn root_is_array(&self) -> bool {
        (self.root.header & JB_FARRAY) != 0
    }
}

/// jbvType: influences sort order. Discriminants matter (used as ordinals).
#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JbvType {
    /* Scalar types */
    Null = 0x0,
    String = 0x1,
    Numeric = 0x2,
    Bool = 0x3,
    /* Composite types */
    Array = 0x10,
    Object = 0x11,
    /* Binary (i.e. struct Jsonb) array/object */
    Binary = 0x12,
    /* Virtual: in-memory only, serialized to json/jsonb on output */
    Datetime = 0x20,
}

/// JsonbValue: in-memory deserialized representation of Jsonb. The C tagged union
/// of `type` + `val` maps to a Rust enum carrying each variant's payload.
pub enum JsonbValue {
    Null,
    String {
        len: i32,
        val: *mut u8, // not necessarily null-terminated; TODO(ptr)
    },
    Numeric(Numeric),
    Bool(bool),
    Array {
        n_elems: i32,
        elems: *mut Self, // TODO(ptr)
        raw_scalar: bool,       // top-level "raw scalar" array?
    },
    Object {
        n_pairs: i32, // 1 pair, 2 elements
        pairs: *mut JsonbPair, // TODO(ptr)
    },
    Binary {
        len: i32,
        data: *mut JsonbContainer, // array or object, on-disk format; TODO(ptr)
    },
    Datetime {
        value: Datum,
        typid: Oid,
        typmod: i32,
        tz: i32, // numeric time zone in seconds, for TimestampTz
    },
}

impl JsonbValue {
    /// IsAJsonbScalar: a scalar type or a datetime.
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            Self::Null
                | Self::String { .. }
                | Self::Numeric(_)
                | Self::Bool(_)
                | Self::Datetime { .. }
        )
    }
}

/// Key/value pair within an Object. Only used while constructing a Jsonb; not the
/// on-disk representation. In-memory.
pub struct JsonbPair {
    pub key: JsonbValue,   // must be a String
    pub value: JsonbValue, // any type
    pub order: u32,        // index in original sequence (for dedup)
}

/// Conversion state used when parsing Jsonb from text, or for type coercion.
pub struct JsonbParseState {
    pub cont_val: JsonbValue,
    pub size: usize,
    pub next: Option<Box<Self>>,
    pub unique_keys: bool, // check object key uniqueness
    pub skip_nulls: bool,  // skip null object fields
}

/// JsonbIterator phase. Sequential ordinal.
pub enum JsonbIterState {
    ArrayStart,
    ArrayElem,
    ObjectStart,
    ObjectKey,
    ObjectValue,
}

/// Holds per-iteration type details plus a view into the Jsonb varlena buffer.
/// In-memory. TODO(ptr): raw pointers borrow the iterated container/buffer.
pub struct JsonbIterator {
    pub container: *mut JsonbContainer,
    pub n_elems: u32, // children count (nPairs for objects)
    pub is_scalar: bool,
    pub children: *mut JEntry,
    pub data_proper: *mut u8, // start of the variable-length data
    pub cur_index: i32,
    pub cur_data_offset: u32,
    pub cur_value_offset: u32, // current value offset when iterating an object
    pub state: JsonbIterState,
    pub parent: Option<Box<Self>>,
}

// Convenience Datum<->Jsonb helpers. TODO(ptr): detoasting returns owned/borrowed
// varlena; PG_DETOAST_DATUM not yet ported.
pub fn DatumGetJsonbP(_d: Datum) -> *mut Jsonb {
    unimplemented!()
}
pub fn DatumGetJsonbPCopy(_d: Datum) -> *mut Jsonb {
    unimplemented!()
}
pub fn JsonbPGetDatum(p: *const Jsonb) -> Datum {
    Datum(p as usize)
}

/* Support functions */
pub fn getJsonbOffset(_jc: &JsonbContainer, _index: i32) -> u32 {
    unimplemented!()
}
pub fn getJsonbLength(_jc: &JsonbContainer, _index: i32) -> u32 {
    unimplemented!()
}
pub fn compareJsonbContainers(_a: &JsonbContainer, _b: &JsonbContainer) -> i32 {
    unimplemented!()
}
// found-or-not -> Option per function-mapping.
pub fn findJsonbValueFromContainer(
    _container: &JsonbContainer,
    _flags: u32,
    _key: &JsonbValue,
) -> Option<JsonbValue> {
    unimplemented!()
}
pub fn getKeyJsonValueFromContainer(
    _container: &JsonbContainer,
    _key_val: &str,
) -> Option<JsonbValue> {
    unimplemented!()
}
pub fn getIthJsonbValueFromContainer(_container: &JsonbContainer, _i: u32) -> Option<JsonbValue> {
    unimplemented!()
}
pub fn pushJsonbValue(
    _pstate: &mut Option<Box<JsonbParseState>>,
    _seq: JsonbIteratorToken,
    _jbval: Option<&JsonbValue>,
) -> Option<JsonbValue> {
    unimplemented!()
}
pub fn JsonbIteratorInit(_container: &JsonbContainer) -> Box<JsonbIterator> {
    unimplemented!()
}
// Mutates the iterator chain (it -> *it); takes &mut Option<Box<..>>.
pub fn JsonbIteratorNext(
    _it: &mut Option<Box<JsonbIterator>>,
    _val: &mut JsonbValue,
    _skip_nested: bool,
) -> JsonbIteratorToken {
    unimplemented!()
}
pub fn JsonbToJsonbValue(_jsonb: &Jsonb, _val: &mut JsonbValue) {
    unimplemented!()
}
pub fn JsonbValueToJsonb(_val: &JsonbValue) -> *mut Jsonb {
    unimplemented!()
}
pub fn JsonbDeepContains(
    _val: &mut Option<Box<JsonbIterator>>,
    _m_contained: &mut Option<Box<JsonbIterator>>,
) -> bool {
    unimplemented!()
}
// Out-param hash -> return value.
pub fn JsonbHashScalarValue(_scalar_val: &JsonbValue) -> u32 {
    unimplemented!()
}
pub fn JsonbHashScalarValueExtended(_scalar_val: &JsonbValue, _seed: u64) -> u64 {
    unimplemented!()
}

/* jsonb.c support functions. StringInfo -> String/Vec<u8> (tombstoned). */
pub fn JsonbToCString(_out: &mut String, _in_: &JsonbContainer, _estimated_len: i32) -> String {
    unimplemented!()
}
pub fn JsonbToCStringIndent(
    _out: &mut String,
    _in_: &JsonbContainer,
    _estimated_len: i32,
) -> String {
    unimplemented!()
}
pub fn JsonbUnquote(_jb: &Jsonb) -> String {
    unimplemented!()
}
pub fn JsonbExtractScalar(_jbc: &JsonbContainer, _res: &mut JsonbValue) -> bool {
    unimplemented!()
}
pub fn JsonbTypeName(_val: &JsonbValue) -> &'static str {
    unimplemented!()
}

pub fn jsonb_set_element(
    _jb: &Jsonb,
    _path: &[Datum],
    _path_len: i32,
    _newval: &JsonbValue,
) -> Datum {
    unimplemented!()
}
// out-param isnull -> (Datum, bool).
pub fn jsonb_get_element(
    _jb: &Jsonb,
    _path: &[Datum],
    _npath: i32,
    _as_text: bool,
) -> (Datum, bool) {
    unimplemented!()
}
pub fn to_jsonb_is_immutable(_typoid: Oid) -> bool {
    unimplemented!()
}
pub fn jsonb_build_object_worker(
    _args: &[Datum],
    _nulls: &[bool],
    _types: &[Oid],
    _absent_on_null: bool,
    _unique_keys: bool,
) -> Datum {
    unimplemented!()
}
pub fn jsonb_build_array_worker(
    _args: &[Datum],
    _nulls: &[bool],
    _types: &[Oid],
    _absent_on_null: bool,
) -> Datum {
    unimplemented!()
}
