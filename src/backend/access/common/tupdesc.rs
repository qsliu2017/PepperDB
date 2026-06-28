//! POSTGRES tuple descriptor support code. Translated from
//! backend/access/common/tupdesc.c.
//!
//! A `TupleDesc` describes the shape of a tuple: the per-attribute
//! `FormData_pg_attribute` rows, a parallel array of `CompactAttribute` cache
//! entries used for fast deformation, the composite type identity, and optional
//! constraints/defaults. The C code allocates the descriptor, its compact-attr
//! array, and its attribute array as one `palloc` block with two flexible-array
//! tails; the header (`crate::access::tupdesc`) instead models those tails as two
//! owned `Vec`s on `TupleDescData`. `TupleDescData` is a fixed-size, `Vec`-backed
//! struct (not a flexible-array DST), so it is owned by value: the create
//! functions return it by value and `free_tuple_desc` takes it by value to
//! reclaim it -- no `unsafe`, no bare pointers.
//!
//! The operations are inherent methods on `TupleDescData` reached through
//! `&`/`&mut` (the `Box` derefs automatically), so every call site shows which
//! borrow it uses. The header keeps each C-named free function as a
//! `#[deprecated] #[inline]` shim taking `&`/`&mut TupleDescData` (or the owning
//! handle for create/free), so existing `crate::access::tupdesc::CreateTupleDesc`
//! call sites keep resolving while new code is nudged toward the methods.
#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct; the real GETSTRUCT returns a MAXALIGN'd pointer (staged until it lands)"
)]

use crate::access::attnum::AttrNumber;
use crate::access::htup::HeapTupleIsValid;
use crate::access::htup_details::GETSTRUCT;
use crate::access::tupdesc::{
    AttrDefault, CompactAttribute, ConstrCheck, TupleConstr, TupleDescData, ATTNULLABLE_UNKNOWN,
    ATTNULLABLE_UNRESTRICTED, ATTNULLABLE_VALID,
};
use crate::access::tupdesc_details::AttrMissing;
use crate::access::toast_compression::INVALID_COMPRESSION_METHOD;
use crate::c::{varlena, NameData, NameStr, NAMEDATALEN, PG_INT16_MAX};
use crate::catalog::catalog::IsCatalogRelationOid;
use crate::catalog::genbki::{
    BOOLOID, DEFAULT_COLLATION_OID, INT4OID, INT8OID, OIDOID, RECORDOID, TEXTARRAYOID, TEXTOID,
};
use crate::catalog::pg_attribute::{FormData_pg_attribute, Form_pg_attribute, ATTRIBUTE_FIXED_PART_SIZE};
use crate::catalog::pg_type::{
    FormData_pg_type, Form_pg_type, TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT,
    TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN,
};
use crate::common::hashfn::hash_uint32;
use crate::nodes::nodes::{stringToNode, Node};
use crate::pg_config::{ALIGNOF_DOUBLE, ALIGNOF_INT, ALIGNOF_SHORT, FLOAT8PASSBYVAL};
use crate::postgres::{DatumGetUInt32, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::nodes::parsenodes::AclMode;
use crate::utils::acl::AclItem;
use crate::utils::datum::{datum_copy, datum_is_equal};
use crate::utils::syscache::{ReleaseSysCache, SearchSysCache1, SysCacheIdentifier};
use crate::elog;

/// `hash_combine` from `common/hashfn.h`: fold a 32-bit value into a running
/// hash. Kept file-local (the header models only the raw hash routines).
#[inline]
const fn hash_combine(a: u32, b: u32) -> u32 {
    // PG hashutils.h: a ^= b + 0x49a0f4dd + (a << 6) + (a >> 2);
    a ^ b
        .wrapping_add(0x49a0_f4dd)
        .wrapping_add(a << 6)
        .wrapping_add(a >> 2)
}

/// Copy the fixed part (`ATTRIBUTE_FIXED_PART_SIZE` bytes) of one attribute row
/// into another, byte-for-byte as the C `memcpy` does. The variable-length tail
/// is not part of an in-memory tupdesc, so a full struct copy is equivalent here.
#[inline]
fn copy_attr_fixed(dst: &mut FormData_pg_attribute, src: &FormData_pg_attribute) {
    let bytes = ATTRIBUTE_FIXED_PART_SIZE;
    debug_assert!(bytes <= core::mem::size_of::<FormData_pg_attribute>());
    // SAFETY: both are valid `FormData_pg_attribute`; the first `bytes` bytes are
    // the fixed part. `#[repr(C)]` makes the layout stable, and a plain-old-data
    // attribute row has no Drop, so a byte copy is sound (mirrors C memcpy).
    unsafe {
        core::ptr::copy_nonoverlapping(
            core::ptr::from_ref::<FormData_pg_attribute>(src).cast::<u8>(),
            core::ptr::from_mut::<FormData_pg_attribute>(dst).cast::<u8>(),
            bytes,
        );
    }
}

/// Write `name` into a `NameData`, NUL-padded to `NAMEDATALEN` (C `namestrcpy`).
fn namestrcpy(name: &mut NameData, src: &str) {
    let bytes = src.as_bytes();
    let n = bytes.len().min(NAMEDATALEN - 1);
    name.data = [0u8; NAMEDATALEN];
    name.data[..n].copy_from_slice(&bytes[..n]);
}

impl TupleDescData {
    /// Total number of attributes.
    pub(crate) fn natts_usize(&self) -> usize {
        usize::try_from(self.natts).unwrap_or(0)
    }

    /// `populate_compact_attribute_internal`: fill `dst` from a source attribute.
    fn populate_compact_attribute_internal(src: &FormData_pg_attribute, dst: &mut CompactAttribute) {
        *dst = CompactAttribute {
            attcacheoff: -1,
            attlen: src.attlen,
            attbyval: src.attbyval,
            attispackable: src.attstorage != TYPSTORAGE_PLAIN,
            atthasmissing: src.atthasmissing,
            attisdropped: src.attisdropped,
            attgenerated: src.attgenerated != b'\0' as i8,
            attnullability: if !src.attnotnull {
                ATTNULLABLE_UNRESTRICTED
            } else if IsCatalogRelationOid(src.attrelid) {
                ATTNULLABLE_VALID
            } else {
                ATTNULLABLE_UNKNOWN
            },
            attalignby: match src.attalign {
                a if a == TYPALIGN_INT => u8::try_from(ALIGNOF_INT).unwrap_or(0),
                a if a == TYPALIGN_CHAR => 1,
                a if a == TYPALIGN_DOUBLE => u8::try_from(ALIGNOF_DOUBLE).unwrap_or(0),
                a if a == TYPALIGN_SHORT => u8::try_from(ALIGNOF_SHORT).unwrap_or(0),
                other => {
                    elog!(
                        crate::utils::elog::ERROR,
                        format!("invalid attalign value: {}", other as u8 as char)
                    );
                    0
                }
            },
        };
    }

    /// `populate_compact_attribute`: refresh the compact entry for attribute
    /// `attnum` (0-based) from its `FormData_pg_attribute`. Must be called after
    /// any change to a `FormData_pg_attribute` in the descriptor.
    pub(crate) fn populate_compact_attribute(&mut self, attnum: usize) {
        let mut tmp = empty_compact_attribute();
        Self::populate_compact_attribute_internal(&self.attrs[attnum], &mut tmp);
        self.compact_attrs[attnum] = tmp;
    }

    /// `verify_compact_attribute`: in debug builds, assert the cached compact
    /// entry matches a freshly-populated one (ignoring `attcacheoff` and
    /// `attnullability`, which are maintained separately).
    pub(crate) fn verify_compact_attribute(&self, attnum: usize) {
        if cfg!(debug_assertions) {
            let cattr = &self.compact_attrs[attnum];
            let mut tmp = empty_compact_attribute();
            Self::populate_compact_attribute_internal(&self.attrs[attnum], &mut tmp);
            tmp.attcacheoff = cattr.attcacheoff;
            tmp.attnullability = cattr.attnullability;
            crate::assert!(compact_attribute_eq(&tmp, cattr));
        }
    }

    /// `CreateTemplateTupleDesc`: allocate an empty descriptor for `natts`
    /// attributes, set up as an anonymous record type and not reference-counted.
    pub fn create_template(natts: i32) -> Self {
        crate::assert!(natts >= 0);
        let n = usize::try_from(natts).unwrap_or(0);

        let mut attrs = Vec::with_capacity(n);
        let mut compact_attrs = Vec::with_capacity(n);
        for _ in 0..n {
            attrs.push(empty_form_attribute());
            compact_attrs.push(empty_compact_attribute());
        }

        Self {
            natts,
            tdtypeid: RECORDOID,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs,
            attrs,
        }
    }

    /// `CreateTupleDesc`: build a descriptor by copying an array of attribute
    /// rows (the C signature takes `Form_pg_attribute *`, an array of pointers).
    pub fn create(natts: i32, attrs: &[Form_pg_attribute]) -> Self {
        let mut desc = Self::create_template(natts);
        for (i, &src) in attrs.iter().enumerate().take(desc.natts_usize()) {
            // SAFETY: caller supplies valid attribute-row pointers (C contract).
            let src_ref = unsafe { &*src };
            copy_attr_fixed(&mut desc.attrs[i], src_ref);
            desc.populate_compact_attribute(i);
        }
        desc
    }

    /// `CreateTupleDescCopy`: copy an existing descriptor's attribute array,
    /// dropping constraints/defaults and the per-column constraint flags.
    #[must_use]
    pub fn create_copy(&self) -> Self {
        let mut desc = Self::create_template(self.natts);
        for i in 0..desc.natts_usize() {
            desc.attrs[i] = clone_form_attribute(&self.attrs[i]);
            let att = &mut desc.attrs[i];
            att.attnotnull = false;
            att.atthasdef = false;
            att.atthasmissing = false;
            att.attidentity = b'\0' as i8;
            att.attgenerated = b'\0' as i8;
            desc.populate_compact_attribute(i);
        }
        desc.tdtypeid = self.tdtypeid;
        desc.tdtypmod = self.tdtypmod;
        desc
    }

    /// `CreateTupleDescTruncatedCopy`: like `create_copy` but keeping only the
    /// first `natts` attributes.
    #[must_use]
    pub fn create_truncated_copy(&self, natts: i32) -> Self {
        crate::assert!(natts <= self.natts);
        let mut desc = Self::create_template(natts);
        for i in 0..desc.natts_usize() {
            desc.attrs[i] = clone_form_attribute(&self.attrs[i]);
            let att = &mut desc.attrs[i];
            att.attnotnull = false;
            att.atthasdef = false;
            att.atthasmissing = false;
            att.attidentity = b'\0' as i8;
            att.attgenerated = b'\0' as i8;
            desc.populate_compact_attribute(i);
        }
        desc.tdtypeid = self.tdtypeid;
        desc.tdtypmod = self.tdtypmod;
        desc
    }

    /// `CreateTupleDescCopyConstr`: copy an existing descriptor including its
    /// constraints and defaults.
    #[must_use]
    pub fn create_copy_constr(&self) -> Self {
        let mut desc = Self::create_template(self.natts);
        for i in 0..desc.natts_usize() {
            desc.attrs[i] = clone_form_attribute(&self.attrs[i]);
            desc.populate_compact_attribute(i);
            desc.compact_attrs[i].attnullability = self.compact_attrs[i].attnullability;
        }

        if let Some(constr) = self.constr.as_deref() {
            let defval: Vec<AttrDefault> = constr
                .defval
                .iter()
                .map(|d| AttrDefault {
                    adnum: d.adnum,
                    adbin: d.adbin.clone(),
                })
                .collect();

            let missing = constr.missing.as_ref().map(|src| {
                src.iter()
                    .enumerate()
                    .map(|(i, m)| {
                        if m.present {
                            let cattr = &self.compact_attrs[i];
                            AttrMissing {
                                present: true,
                                value: datum_copy(m.value, cattr.attbyval, i32::from(cattr.attlen)),
                            }
                        } else {
                            AttrMissing {
                                present: m.present,
                                value: m.value,
                            }
                        }
                    })
                    .collect()
            });

            let check: Vec<ConstrCheck> = constr
                .check
                .iter()
                .map(|c| ConstrCheck {
                    ccname: c.ccname.clone(),
                    ccbin: c.ccbin.clone(),
                    ccenforced: c.ccenforced,
                    ccvalid: c.ccvalid,
                    ccnoinherit: c.ccnoinherit,
                })
                .collect();

            desc.constr = Some(Box::new(TupleConstr {
                defval,
                check,
                missing,
                has_not_null: constr.has_not_null,
                has_generated_stored: constr.has_generated_stored,
                has_generated_virtual: constr.has_generated_virtual,
            }));
        }

        desc.tdtypeid = self.tdtypeid;
        desc.tdtypmod = self.tdtypmod;
        desc
    }

    /// `TupleDescCopy`: copy `self` into the caller-supplied descriptor `dst`,
    /// dropping constraints/defaults and clearing the destination's refcount.
    pub fn copy_into(&self, dst: &mut Self) {
        dst.natts = self.natts;
        dst.tdtypeid = self.tdtypeid;
        dst.tdtypmod = self.tdtypmod;
        dst.attrs = self.attrs.iter().map(clone_form_attribute).collect();
        dst.compact_attrs = self.compact_attrs.iter().map(clone_compact_attribute).collect();

        for i in 0..dst.natts_usize() {
            let att = &mut dst.attrs[i];
            att.attnotnull = false;
            att.atthasdef = false;
            att.atthasmissing = false;
            att.attidentity = b'\0' as i8;
            att.attgenerated = b'\0' as i8;
            dst.populate_compact_attribute(i);
        }
        dst.constr = None;
        dst.tdrefcount = -1;
    }

    /// `TupleDescCopyEntry`: copy a single attribute (1-based `src_attno`) into
    /// `dst` at 1-based `dst_attno`, dropping constraints/defaults.
    pub fn copy_entry(
        dst: &mut Self,
        dst_attno: AttrNumber,
        src: &Self,
        src_attno: AttrNumber,
    ) {
        crate::assert!(src_attno >= 1);
        crate::assert!(src_attno <= src.natts as AttrNumber);
        crate::assert!(dst_attno >= 1);
        crate::assert!(dst_attno <= dst.natts as AttrNumber);

        let src_idx = usize::try_from(src_attno - 1).unwrap_or(0);
        let dst_idx = usize::try_from(dst_attno - 1).unwrap_or(0);

        copy_attr_fixed(&mut dst.attrs[dst_idx], &src.attrs[src_idx]);

        let att = &mut dst.attrs[dst_idx];
        att.attnum = dst_attno;
        att.attnotnull = false;
        att.atthasdef = false;
        att.atthasmissing = false;
        att.attidentity = b'\0' as i8;
        att.attgenerated = b'\0' as i8;

        dst.populate_compact_attribute(dst_idx);
    }

    /// `equalTupleDescs`: full logical equality (attributes plus constraints).
    pub fn equals(&self, other: &Self) -> bool {
        if self.natts != other.natts {
            return false;
        }
        if self.tdtypeid != other.tdtypeid {
            return false;
        }

        for i in 0..self.natts_usize() {
            let attr1 = &self.attrs[i];
            let attr2 = &other.attrs[i];

            if NameStr(&attr1.attname) != NameStr(&attr2.attname) {
                return false;
            }
            if attr1.atttypid != attr2.atttypid
                || attr1.attlen != attr2.attlen
                || attr1.attndims != attr2.attndims
                || attr1.atttypmod != attr2.atttypmod
                || attr1.attbyval != attr2.attbyval
                || attr1.attalign != attr2.attalign
                || attr1.attstorage != attr2.attstorage
                || attr1.attcompression != attr2.attcompression
                || attr1.attnotnull != attr2.attnotnull
            {
                return false;
            }

            if attr1.attnotnull {
                let cattr1 = &self.compact_attrs[i];
                let cattr2 = &other.compact_attrs[i];
                crate::assert!(cattr1.attnullability != ATTNULLABLE_UNKNOWN);
                crate::assert!(
                    (cattr1.attnullability == ATTNULLABLE_UNKNOWN)
                        == (cattr2.attnullability == ATTNULLABLE_UNKNOWN)
                );
                if cattr1.attnullability != cattr2.attnullability {
                    return false;
                }
            }
            if attr1.atthasdef != attr2.atthasdef
                || attr1.attidentity != attr2.attidentity
                || attr1.attgenerated != attr2.attgenerated
                || attr1.attisdropped != attr2.attisdropped
                || attr1.attislocal != attr2.attislocal
                || attr1.attinhcount != attr2.attinhcount
                || attr1.attcollation != attr2.attcollation
            {
                return false;
            }
        }

        match (self.constr.as_deref(), other.constr.as_deref()) {
            (None, None) => true,
            (None, Some(_)) | (Some(_), None) => false,
            (Some(constr1), Some(constr2)) => self.constr_equals(constr1, other, constr2),
        }
    }

    /// The constraint comparison half of `equalTupleDescs`.
    fn constr_equals(
        &self,
        constr1: &TupleConstr,
        other: &Self,
        constr2: &TupleConstr,
    ) -> bool {
        if constr1.has_not_null != constr2.has_not_null
            || constr1.has_generated_stored != constr2.has_generated_stored
            || constr1.has_generated_virtual != constr2.has_generated_virtual
        {
            return false;
        }
        if constr1.defval.len() != constr2.defval.len() {
            return false;
        }
        // AttrDefault arrays are assumed in adnum order.
        for (d1, d2) in constr1.defval.iter().zip(constr2.defval.iter()) {
            if d1.adnum != d2.adnum || d1.adbin != d2.adbin {
                return false;
            }
        }

        match (constr1.missing.as_ref(), constr2.missing.as_ref()) {
            (Some(_), None) | (None, Some(_)) => return false,
            (None, None) => {}
            (Some(m1), Some(m2)) => {
                for i in 0..self.natts_usize() {
                    let mv1 = &m1[i];
                    let mv2 = &m2[i];
                    if mv1.present != mv2.present {
                        return false;
                    }
                    if mv1.present {
                        let cattr1 = &self.compact_attrs[i];
                        let _ = other;
                        if !datum_is_equal(
                            mv1.value,
                            mv2.value,
                            cattr1.attbyval,
                            i32::from(cattr1.attlen),
                        ) {
                            return false;
                        }
                    }
                }
            }
        }

        if constr1.check.len() != constr2.check.len() {
            return false;
        }
        // ConstrCheck entries are assumed sorted by name.
        constr1.check.iter().zip(constr2.check.iter()).all(|(c1, c2)| {
            c1.ccname == c2.ccname
                && c1.ccbin == c2.ccbin
                && c1.ccenforced == c2.ccenforced
                && c1.ccvalid == c2.ccvalid
                && c1.ccnoinherit == c2.ccnoinherit
        })
    }

    /// `equalRowTypes`: row-type equality (name/type/typmod/collation/dropped),
    /// ignoring physical-storage and table-metadata fields and `tdtypmod`.
    pub fn row_types_equal(&self, other: &Self) -> bool {
        if self.natts != other.natts || self.tdtypeid != other.tdtypeid {
            return false;
        }
        (0..self.natts_usize()).all(|i| {
            let a1 = &self.attrs[i];
            let a2 = &other.attrs[i];
            NameStr(&a1.attname) == NameStr(&a2.attname)
                && a1.atttypid == a2.atttypid
                && a1.atttypmod == a2.atttypmod
                && a1.attcollation == a2.attcollation
                && a1.attisdropped == a2.attisdropped
        })
    }

    /// `hashRowType`: a hash consistent with `row_types_equal`.
    pub fn hash_row_type(&self) -> u32 {
        let natts_u32 = u32::try_from(self.natts).unwrap_or(0);
        let mut s = hash_combine(0, DatumGetUInt32(hash_uint32(natts_u32)));
        s = hash_combine(s, DatumGetUInt32(hash_uint32(self.tdtypeid.0)));
        for attr in &self.attrs {
            s = hash_combine(s, DatumGetUInt32(hash_uint32(attr.atttypid.0)));
        }
        s
    }

    /// `TupleDescInitEntry`: initialize attribute `attribute_number` (1-based)
    /// from a catalog type lookup. `attribute_name` of `None` sets an empty name.
    pub fn init_entry(
        &mut self,
        attribute_number: AttrNumber,
        attribute_name: Option<&str>,
        oidtypeid: Oid,
        typmod: i32,
        attdim: i32,
    ) {
        crate::assert!(attribute_number >= 1);
        crate::assert!(attribute_number <= self.natts as AttrNumber);
        crate::assert!(attdim >= 0);
        crate::assert!(attdim <= i32::from(PG_INT16_MAX));

        let idx = usize::try_from(attribute_number - 1).unwrap_or(0);

        {
            let att = &mut self.attrs[idx];
            att.attrelid = InvalidOid; // dummy value
            match attribute_name {
                None => att.attname.data = [0u8; NAMEDATALEN],
                Some(name) => namestrcpy(&mut att.attname, name),
            }
            att.atttypmod = typmod;
            att.attnum = attribute_number;
            att.attndims = i16::try_from(attdim).unwrap_or(0);
            att.attnotnull = false;
            att.atthasdef = false;
            att.atthasmissing = false;
            att.attidentity = b'\0' as i8;
            att.attgenerated = b'\0' as i8;
            att.attisdropped = false;
            att.attislocal = true;
            att.attinhcount = 0;
        }

        let tuple = SearchSysCache1(SysCacheIdentifier::TYPEOID, ObjectIdGetDatum(oidtypeid));
        // SAFETY: SearchSysCache1 returns a valid HeapTuple pointer or None.
        let tuple_ref = tuple.map(|t| unsafe { &*t });
        if !HeapTupleIsValid(tuple_ref) {
            elog!(
                crate::utils::elog::ERROR,
                format!("cache lookup failed for type {}", oidtypeid.0)
            );
        }
        let tuple = tuple.unwrap_or_else(|| {
            elog!(
                crate::utils::elog::ERROR,
                format!("cache lookup failed for type {}", oidtypeid.0)
            );
            core::ptr::null_mut()
        });
        // SAFETY: tuple is a valid HeapTuple (checked above); GETSTRUCT yields the
        // Form_pg_type body, valid while the syscache entry is held.
        let type_form: Form_pg_type = unsafe { GETSTRUCT(&*tuple) }.cast::<FormData_pg_type>();
        // SAFETY: type_form points at a live Form_pg_type from the held tuple.
        let type_form = unsafe { &*type_form };

        {
            let att = &mut self.attrs[idx];
            att.atttypid = oidtypeid;
            att.attlen = type_form.typlen;
            att.attbyval = type_form.typbyval;
            att.attalign = type_form.typalign;
            att.attstorage = type_form.typstorage;
            att.attcompression = INVALID_COMPRESSION_METHOD as i8;
            att.attcollation = type_form.typcollation;
        }

        self.populate_compact_attribute(idx);

        ReleaseSysCache(tuple);
    }

    /// `TupleDescInitBuiltinEntry`: initialize an attribute for one of a small
    /// set of builtin types without catalog access (for catalog-less processes).
    pub fn init_builtin_entry(
        &mut self,
        attribute_number: AttrNumber,
        attribute_name: &str,
        oidtypeid: Oid,
        typmod: i32,
        attdim: i32,
    ) {
        crate::assert!(attribute_number >= 1);
        crate::assert!(attribute_number <= self.natts as AttrNumber);
        crate::assert!(attdim >= 0);
        crate::assert!(attdim <= i32::from(PG_INT16_MAX));

        let idx = usize::try_from(attribute_number - 1).unwrap_or(0);

        {
            let att = &mut self.attrs[idx];
            att.attrelid = InvalidOid; // dummy value
            namestrcpy(&mut att.attname, attribute_name);
            att.atttypmod = typmod;
            att.attnum = attribute_number;
            att.attndims = i16::try_from(attdim).unwrap_or(0);
            att.attnotnull = false;
            att.atthasdef = false;
            att.atthasmissing = false;
            att.attidentity = b'\0' as i8;
            att.attgenerated = b'\0' as i8;
            att.attisdropped = false;
            att.attislocal = true;
            att.attinhcount = 0;
            att.atttypid = oidtypeid;
        }

        // Only a limited set of builtin types is supported.
        let att = &mut self.attrs[idx];
        if oidtypeid == TEXTOID || oidtypeid == TEXTARRAYOID {
            att.attlen = -1;
            att.attbyval = false;
            att.attalign = TYPALIGN_INT;
            att.attstorage = TYPSTORAGE_EXTENDED;
            att.attcompression = INVALID_COMPRESSION_METHOD as i8;
            att.attcollation = DEFAULT_COLLATION_OID;
        } else if oidtypeid == BOOLOID {
            att.attlen = 1;
            att.attbyval = true;
            att.attalign = TYPALIGN_CHAR;
            att.attstorage = TYPSTORAGE_PLAIN;
            att.attcompression = INVALID_COMPRESSION_METHOD as i8;
            att.attcollation = InvalidOid;
        } else if oidtypeid == INT4OID {
            att.attlen = 4;
            att.attbyval = true;
            att.attalign = TYPALIGN_INT;
            att.attstorage = TYPSTORAGE_PLAIN;
            att.attcompression = INVALID_COMPRESSION_METHOD as i8;
            att.attcollation = InvalidOid;
        } else if oidtypeid == INT8OID {
            att.attlen = 8;
            att.attbyval = FLOAT8PASSBYVAL;
            att.attalign = TYPALIGN_DOUBLE;
            att.attstorage = TYPSTORAGE_PLAIN;
            att.attcompression = INVALID_COMPRESSION_METHOD as i8;
            att.attcollation = InvalidOid;
        } else if oidtypeid == OIDOID {
            att.attlen = 4;
            att.attbyval = true;
            att.attalign = TYPALIGN_INT;
            att.attstorage = TYPSTORAGE_PLAIN;
            att.attcompression = INVALID_COMPRESSION_METHOD as i8;
            att.attcollation = InvalidOid;
        } else {
            elog!(
                crate::utils::elog::ERROR,
                format!("unsupported type {}", oidtypeid.0)
            );
        }

        self.populate_compact_attribute(idx);
    }

    /// `TupleDescInitEntryCollation`: assign a nondefault collation to an
    /// already-initialized entry (1-based `attribute_number`).
    pub fn init_entry_collation(&mut self, attribute_number: AttrNumber, collationid: Oid) {
        crate::assert!(attribute_number >= 1);
        crate::assert!(attribute_number <= self.natts as AttrNumber);
        let idx = usize::try_from(attribute_number - 1).unwrap_or(0);
        self.attrs[idx].attcollation = collationid;
    }

    /// `TupleDescGetDefault`: the default expression node for `attnum`, if any.
    /// C returns a `Node *` (NULL if none); here `Option<Node>`.
    pub fn get_default(&self, attnum: AttrNumber) -> Option<Node> {
        let constr = self.constr.as_deref()?;
        let adbin = constr
            .defval
            .iter()
            .find(|d| d.adnum == attnum)
            .map(|d| d.adbin.as_str())?;
        let node = stringToNode(adbin);
        if node.is_null() {
            None
        } else {
            // SAFETY: stringToNode returns an owned Node (Box::into_raw) when
            // non-null; reclaim it. (Currently the read subsystem is a stub, so
            // this path is unreachable until nodes/read.c is translated.)
            Some(*unsafe { Box::from_raw(node.cast::<Node>()) })
        }
    }
}

/// `FreeTupleDesc`: free a descriptor and all its substructure. Taking the
/// descriptor by value reclaims it; its `Vec`s and `Box<TupleConstr>` drop with
/// it. Mirrors the C entry point and asserts the refcount.
pub fn free_tuple_desc(tupdesc: TupleDescData) {
    crate::assert!(tupdesc.tdrefcount <= 0);
    drop(tupdesc);
}

/// `IncrTupleDescRefCount`: bump a reference-counted descriptor's refcount.
///
/// TODO(resowner): C also logs the reference in `CurrentResourceOwner` so an
/// `ERROR` unwind releases it. That registration needs a second owning handle to
/// the descriptor, which only exists once the handle graduates from `Box` (unique
/// ownership) to `Arc<TupleDescData>` at the relcache/typcache milestone. Until
/// then this maintains only the manual counter (the descriptors that reach M1 are
/// not yet shared or resource-owner tracked).
pub fn incr_tuple_desc_ref_count(tupdesc: &mut TupleDescData) {
    crate::assert!(tupdesc.tdrefcount >= 0);
    tupdesc.tdrefcount += 1;
}

/// `DecrTupleDescRefCount`: drop a reference taken by `incr_tuple_desc_ref_count`.
///
/// TODO(resowner): see `incr_tuple_desc_ref_count`. With unique `Box` ownership
/// the descriptor cannot be freed from here (the owner holds the `Box`); this
/// adjusts the manual counter only. Freeing at zero is reinstated with the `Arc`
/// handle, when this and `FreeTupleDesc` converge on dropping the last reference.
pub fn decr_tuple_desc_ref_count(tupdesc: &mut TupleDescData) {
    crate::assert!(tupdesc.tdrefcount > 0);
    tupdesc.tdrefcount -= 1;
}

/// `BuildDescFromLists`: build a constraint-free descriptor for a RECORD return
/// type from parallel lists of names, type OIDs, typmods, and collations.
pub fn build_desc_from_lists(
    names: &[String],
    types: &[Oid],
    typmods: &[i32],
    collations: &[Oid],
) -> TupleDescData {
    let natts = names.len();
    crate::assert!(natts == types.len());
    crate::assert!(natts == typmods.len());
    crate::assert!(natts == collations.len());

    let mut desc = TupleDescData::create_template(i32::try_from(natts).unwrap_or(0));
    for i in 0..natts {
        let attnum = AttrNumber::try_from(i + 1).unwrap_or(1);
        desc.init_entry(attnum, Some(names[i].as_str()), types[i], typmods[i], 0);
        desc.init_entry_collation(attnum, collations[i]);
    }
    desc
}

// --- small constructors / comparisons for the owned attribute rows ---

fn empty_compact_attribute() -> CompactAttribute {
    CompactAttribute {
        attcacheoff: -1,
        attlen: 0,
        attbyval: false,
        attispackable: false,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby: 0,
    }
}

fn clone_compact_attribute(c: &CompactAttribute) -> CompactAttribute {
    CompactAttribute {
        attcacheoff: c.attcacheoff,
        attlen: c.attlen,
        attbyval: c.attbyval,
        attispackable: c.attispackable,
        atthasmissing: c.atthasmissing,
        attisdropped: c.attisdropped,
        attgenerated: c.attgenerated,
        attnullability: c.attnullability,
        attalignby: c.attalignby,
    }
}

fn compact_attribute_eq(a: &CompactAttribute, b: &CompactAttribute) -> bool {
    a.attcacheoff == b.attcacheoff
        && a.attlen == b.attlen
        && a.attbyval == b.attbyval
        && a.attispackable == b.attispackable
        && a.atthasmissing == b.atthasmissing
        && a.attisdropped == b.attisdropped
        && a.attgenerated == b.attgenerated
        && a.attnullability == b.attnullability
        && a.attalignby == b.attalignby
}

fn empty_form_attribute() -> FormData_pg_attribute {
    // The CATALOG_VARLEN tail fields (attacl/attoptions/attfdwoptions/
    // attmissingval) are never present in an in-memory tupdesc; we only ever
    // touch the fixed part. Construct them as empty/zero placeholders so the
    // struct (`#[repr(C)]`) is well-formed.
    let empty_varlena = || varlena { vl_len_: [0u8; 4], dat: [] };
    FormData_pg_attribute {
        attrelid: InvalidOid,
        attname: NameData { data: [0u8; NAMEDATALEN] },
        atttypid: InvalidOid,
        attlen: 0,
        attnum: 0,
        atttypmod: -1,
        attndims: 0,
        attbyval: false,
        attalign: 0,
        attstorage: 0,
        attcompression: 0,
        attnotnull: false,
        atthasdef: false,
        atthasmissing: false,
        attidentity: 0,
        attgenerated: 0,
        attisdropped: false,
        attislocal: false,
        attinhcount: 0,
        attcollation: InvalidOid,
        attstattarget: 0,
        attacl: [AclItem {
            grantee: InvalidOid,
            grantor: InvalidOid,
            privs: AclMode::from_bits_retain(0),
        }],
        attoptions: [empty_varlena()],
        attfdwoptions: [empty_varlena()],
        attmissingval: empty_varlena(),
    }
}

fn clone_form_attribute(a: &FormData_pg_attribute) -> FormData_pg_attribute {
    let mut out = empty_form_attribute();
    copy_attr_fixed(&mut out, a);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::ATTNULLABLE_UNRESTRICTED;

    /// Build a descriptor and fill attributes without catalog access.
    fn make_desc(specs: &[(&str, Oid)]) -> TupleDescData {
        let mut desc = TupleDescData::create_template(specs.len() as i32);
        for (i, (name, oid)) in specs.iter().enumerate() {
            desc.init_builtin_entry((i + 1) as AttrNumber, name, *oid, -1, 0);
        }
        desc
    }

    #[test]
    fn create_template_sets_record_defaults() {
        let desc = TupleDescData::create_template(3);
        assert_eq!(desc.natts, 3);
        assert_eq!(desc.tdtypeid, RECORDOID);
        assert_eq!(desc.tdtypmod, -1);
        assert_eq!(desc.tdrefcount, -1);
        assert!(desc.constr.is_none());
        assert_eq!(desc.attrs.len(), 3);
        assert_eq!(desc.compact_attrs.len(), 3);
        free_tuple_desc(desc);
    }

    #[test]
    fn init_builtin_entry_fills_attribute() {
        let desc = make_desc(&[("id", INT4OID), ("flag", BOOLOID)]);

        assert_eq!(NameStr(&desc.attr(0).attname)[..2], *b"id");
        assert_eq!(desc.attr(0).atttypid, INT4OID);
        assert_eq!(desc.attr(0).attlen, 4);
        assert!(desc.attr(0).attbyval);
        assert_eq!(desc.attr(0).attnum, 1);

        assert_eq!(desc.attr(1).atttypid, BOOLOID);
        assert_eq!(desc.attr(1).attlen, 1);

        // Compact attrs are populated to match.
        assert_eq!(desc.compact_attr(0).attlen, 4);
        assert!(desc.compact_attr(0).attbyval);
        assert_eq!(desc.compact_attr(0).attnullability, ATTNULLABLE_UNRESTRICTED);

        free_tuple_desc(desc);
    }

    #[test]
    fn create_copy_matches_attributes() {
        let src = make_desc(&[("a", INT4OID), ("b", INT8OID)]);
        let copy = src.create_copy();

        assert_eq!(copy.natts, src.natts);
        assert_eq!(copy.tdtypeid, src.tdtypeid);
        assert_eq!(NameStr(&copy.attr(0).attname)[..1], *b"a");
        assert_eq!(copy.attr(1).atttypid, INT8OID);

        free_tuple_desc(src);
        free_tuple_desc(copy);
    }

    #[test]
    fn equal_tuple_descs_true_and_false() {
        let a = make_desc(&[("x", INT4OID), ("y", BOOLOID)]);
        let b = make_desc(&[("x", INT4OID), ("y", BOOLOID)]);
        let c = make_desc(&[("x", INT4OID), ("z", BOOLOID)]); // different name
        let d = make_desc(&[("x", INT8OID), ("y", BOOLOID)]); // different type

        assert!(a.equals(&b));
        assert!(!a.equals(&c));
        assert!(!a.equals(&d));

        free_tuple_desc(a);
        free_tuple_desc(b);
        free_tuple_desc(c);
        free_tuple_desc(d);
    }

    #[test]
    fn equal_tuple_descs_natts_mismatch() {
        let a = make_desc(&[("x", INT4OID)]);
        let b = make_desc(&[("x", INT4OID), ("y", BOOLOID)]);
        assert!(!a.equals(&b));
        free_tuple_desc(a);
        free_tuple_desc(b);
    }

    #[test]
    fn row_types_equal_ignores_storage() {
        let a = make_desc(&[("x", INT4OID)]);
        let b = make_desc(&[("x", INT4OID)]);
        assert!(a.row_types_equal(&b));

        let c = make_desc(&[("y", INT4OID)]); // different name
        assert!(!a.row_types_equal(&c));

        free_tuple_desc(a);
        free_tuple_desc(b);
        free_tuple_desc(c);
    }

    // hash_row_type is structurally covered by row_types_equal; its numeric
    // value can only be exercised once common/hashfn's hash_bytes_uint32 lands
    // (an unimplemented!() stub today), so no value-equality test here yet.

    #[test]
    fn copy_into_clears_refcount_and_constr() {
        let src = make_desc(&[("a", INT4OID)]);
        let mut dst = TupleDescData::create_template(1);
        dst.tdrefcount = 5;
        src.copy_into(&mut dst);

        assert_eq!(dst.tdrefcount, -1);
        assert!(dst.constr.is_none());
        assert_eq!(NameStr(&dst.attr(0).attname)[..1], *b"a");

        free_tuple_desc(src);
        free_tuple_desc(dst);
    }

    #[test]
    fn copy_entry_renumbers_target() {
        let src = make_desc(&[("first", INT4OID), ("second", INT8OID)]);
        let mut dst = make_desc(&[("zero", BOOLOID), ("one", BOOLOID)]);

        // copy src attr 2 (1-based) into dst attr 1 (1-based)
        TupleDescData::copy_entry(&mut dst, 1, &src, 2);

        assert_eq!(dst.attr(0).atttypid, INT8OID);
        assert_eq!(dst.attr(0).attnum, 1); // renumbered to dst position

        free_tuple_desc(src);
        free_tuple_desc(dst);
    }

    #[test]
    fn truncated_copy_keeps_prefix() {
        let src = make_desc(&[("a", INT4OID), ("b", INT8OID), ("c", BOOLOID)]);
        let copy = src.create_truncated_copy(2);
        assert_eq!(copy.natts, 2);
        assert_eq!(NameStr(&copy.attr(1).attname)[..1], *b"b");
        free_tuple_desc(src);
        free_tuple_desc(copy);
    }
}
