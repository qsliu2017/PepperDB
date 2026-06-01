//! Translation of postgres/src/backend/access/common/tupdesc.c
//! (merged with the parts of postgres/src/include/access/tupdesc.h and
//! postgres/src/include/access/tupdesc_details.h that it needs).
//!
//! POSTGRES tuple descriptor support code.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping (from tupdesc.c):
//!   postgres.h                    -> crate::prelude
//!   access/htup_details.h         -> STUB (not yet ported; only GETSTRUCT/HeapTuple
//!                                    used by the STUBBED TupleDescInitEntry)
//!   access/toast_compression.h    -> InvalidCompressionMethod / TYPSTORAGE_* defined
//!                                    module-locally below (mirrors toast_compression.h
//!                                    and pg_type.h); also lives in
//!                                    crate::access::common::toast_internals
//!   access/tupdesc_details.h      -> AttrMissing merged in below
//!   catalog/catalog.h             -> STUB (IsCatalogRelationOid -> local stub)
//!   catalog/pg_collation.h        -> crate::catalog::pg_known_oids
//!                                    (DEFAULT_COLLATION_OID / C_COLLATION_OID)
//!   catalog/pg_type.h             -> crate::catalog::pg_type_d (built-in type OIDs);
//!                                    TYPALIGN_*/TYPSTORAGE_* defined locally
//!   common/hashfn.h               -> crate::common::hashfn
//!   utils/builtins.h              -> namestrcpy from crate::utils::adt::name
//!   utils/datum.h                 -> crate::utils::adt::datum (datumCopy/datumIsEqual)
//!   utils/resowner.h              -> STUB (ResourceOwner machinery -> local no-ops)
//!   utils/syscache.h              -> STUB (SearchSysCache1 -> TupleDescInitEntry stubbed)
//!   access/attnum.h               -> AttrNumber from crate::nodes::primnodes
//!   nodes/pg_list.h               -> List from crate::nodes::pg_list (used only by the
//!                                    STUBBED BuildDescFromLists)
//!   (Node, returned by the STUBBED TupleDescGetDefault) -> crate::nodes::nodes::Node
//!
//! WHAT IS REAL vs STUBBED - see the per-function comments; summary:
//!   REAL: populate_compact_attribute(_internal), verify_compact_attribute,
//!     TupleDescAttr/TupleDescCompactAttr, CreateTemplateTupleDesc, CreateTupleDesc,
//!     CreateTupleDescCopy, CreateTupleDescTruncatedCopy, CreateTupleDescCopyConstr,
//!     TupleDescCopy, TupleDescCopyEntry, FreeTupleDesc,
//!     IncrTupleDescRefCount/DecrTupleDescRefCount (ResourceOwner calls are local
//!     no-op stubs; the refcount logic is real), equalTupleDescs, equalRowTypes,
//!     hashRowType, TupleDescInitEntryCollation, TupleDescInitBuiltinEntry.
//!     IsCatalogRelationOid is a local stub returning false (TODO catalog.c) - it only
//!     affects whether a NOT NULL constraint is recorded as VALID vs UNKNOWN.
//!   STUBBED: TupleDescInitEntry (syscache), BuildDescFromLists (parser),
//!     TupleDescGetDefault (the adbin->expr path; the AttrMissing fast-path is N/A here
//!     because AttrMissing is not consulted by TupleDescGetDefault in upstream), and
//!     the ResourceOwner callbacks.

use crate::prelude::*;

use crate::catalog::pg_attribute::{
    ATTRIBUTE_FIXED_PART_SIZE, Form_pg_attribute, FormData_pg_attribute,
};
use crate::catalog::pg_known_oids::DEFAULT_COLLATION_OID;
use crate::catalog::pg_type_d::{BOOLOID, INT4OID, INT8OID, OIDOID, RECORDOID, TEXTARRAYOID, TEXTOID};
use crate::common::hashfn::{hash_combine, hash_uint32};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::AttrNumber;
use crate::pg_config::{ALIGNOF_DOUBLE, ALIGNOF_INT, ALIGNOF_SHORT};
use crate::utils::adt::datum::{datumCopy, datumIsEqual};
use crate::utils::adt::name::namestrcpy;

use core::ffi::{c_char, c_int, c_void};
use core::mem::{offset_of, size_of};

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    #[cfg(debug_assertions)]
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// ----------------------------------------------------------------------------
//   Constants from pg_type.h / toast_compression.h (not-yet-ported headers).
// ----------------------------------------------------------------------------

/* pg_type.h: typalign codes (FormData_pg_attribute.attalign). */
pub const TYPALIGN_CHAR: c_char = b'c' as c_char;
pub const TYPALIGN_SHORT: c_char = b's' as c_char;
pub const TYPALIGN_INT: c_char = b'i' as c_char;
pub const TYPALIGN_DOUBLE: c_char = b'd' as c_char;

/* pg_type.h: typstorage codes (FormData_pg_attribute.attstorage). */
pub const TYPSTORAGE_PLAIN: c_char = b'p' as c_char;
pub const TYPSTORAGE_EXTERNAL: c_char = b'e' as c_char;
pub const TYPSTORAGE_EXTENDED: c_char = b'x' as c_char;
pub const TYPSTORAGE_MAIN: c_char = b'm' as c_char;

/* access/toast_compression.h: '\0' attcompression means "default". */
pub const InvalidCompressionMethod: c_char = b'\0' as c_char;

// ============================================================================
//   tupdesc_details.h
// ============================================================================

/*
 * Structure used to represent value to be used when the attribute is not
 * present at all in a tuple, i.e. when the column was created after the tuple
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AttrMissing {
    /* true if non-NULL missing value exists */
    pub am_present: bool,
    /* value when attribute is missing */
    pub am_value: Datum,
}

// ============================================================================
//   tupdesc.h
// ============================================================================

#[repr(C)]
pub struct AttrDefault {
    pub adnum: AttrNumber,
    /* nodeToString representation of expr */
    pub adbin: *mut c_char,
}

#[repr(C)]
pub struct ConstrCheck {
    pub ccname: *mut c_char,
    /* nodeToString representation of expr */
    pub ccbin: *mut c_char,
    pub ccenforced: bool,
    pub ccvalid: bool,
    /* this is a non-inheritable constraint */
    pub ccnoinherit: bool,
}

/* This structure contains constraints of a tuple */
#[repr(C)]
pub struct TupleConstr {
    /* array */
    pub defval: *mut AttrDefault,
    /* array */
    pub check: *mut ConstrCheck,
    /* missing attributes values, NULL if none */
    pub missing: *mut AttrMissing,
    pub num_defval: uint16,
    pub num_check: uint16,
    /* any not-null, including not valid ones */
    pub has_not_null: bool,
    pub has_generated_stored: bool,
    pub has_generated_virtual: bool,
}

/*
 * CompactAttribute
 *		Cut-down version of FormData_pg_attribute for faster access for tasks
 *		such as tuple deformation.  The fields of this struct are populated
 *		using the populate_compact_attribute() function, which must be called
 *		directly after the FormData_pg_attribute struct is populated or
 *		altered in any way.
 *
 * Currently, this struct is 16 bytes.  Any code changes which enlarge this
 * struct should be considered very carefully.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CompactAttribute {
    /* fixed offset into tuple, if known, or -1 */
    pub attcacheoff: int32,
    /* attr len in bytes or -1 = varlen, -2 = cstring */
    pub attlen: int16,
    /* as FormData_pg_attribute.attbyval */
    pub attbyval: bool,
    /* FormData_pg_attribute.attstorage != TYPSTORAGE_PLAIN */
    pub attispackable: bool,
    /* as FormData_pg_attribute.atthasmissing */
    pub atthasmissing: bool,
    /* as FormData_pg_attribute.attisdropped */
    pub attisdropped: bool,
    /* FormData_pg_attribute.attgenerated != '\0' */
    pub attgenerated: bool,
    /* status of not-null constraint, see below */
    pub attnullability: c_char,
    /* alignment requirement in bytes */
    pub attalignby: uint8,
}

/* Valid values for CompactAttribute->attnullability */
/* No constraint exists */
pub const ATTNULLABLE_UNRESTRICTED: c_char = b'f' as c_char;
/* constraint exists, validity unknown */
pub const ATTNULLABLE_UNKNOWN: c_char = b'u' as c_char;
/* valid constraint exists */
pub const ATTNULLABLE_VALID: c_char = b'v' as c_char;
/* constraint exists, marked invalid */
pub const ATTNULLABLE_INVALID: c_char = b'i' as c_char;

/*
 * This struct is passed around within the backend to describe the structure
 * of tuples.  See the full discussion in tupdesc.h.
 *
 * The TupleDesc is a single palloc'd block laid out as:
 *   [ TupleDescData header ]
 *   [ compact_attrs: CompactAttribute * natts ]
 *   [ FormData_pg_attribute * natts ]
 * The compact_attrs flexible array starts at offset_of!(TupleDescData,
 * compact_attrs); the FormData_pg_attribute array starts immediately after the
 * natts CompactAttributes.  See TupleDescAttr/TupleDescCompactAttr below.
 */
#[repr(C)]
pub struct TupleDescData {
    /* number of attributes in the tuple */
    pub natts: c_int,
    /* composite type ID for tuple type */
    pub tdtypeid: Oid,
    /* typmod for tuple type */
    pub tdtypmod: int32,
    /* reference count, or -1 if not counting */
    pub tdrefcount: c_int,
    /* constraints, or NULL if none */
    pub constr: *mut TupleConstr,
    /* compact_attrs[N] is the compact metadata of Attribute Number N+1 */
    pub compact_attrs: [CompactAttribute; FLEXIBLE_ARRAY_MEMBER],
}
pub type TupleDesc = *mut TupleDescData;

// ----------------------------------------------------------------------------
//   Layout helpers (TupleDescAttr / TupleDescCompactAttr / TupleDescSize), the
//   Rust equivalents of the macros / static inlines in tupdesc.h.
// ----------------------------------------------------------------------------

/*
 * Number of bytes from the start of the TupleDescData block to the end of the
 * compact_attrs array - i.e. the offset of the FormData_pg_attribute array.
 *
 * Equivalent to the C expression:
 *   offsetof(struct TupleDescData, compact_attrs) +
 *   natts * sizeof(CompactAttribute)
 */
#[inline]
const fn tupledesc_header_and_compact(natts: c_int) -> usize {
    offset_of!(TupleDescData, compact_attrs) + (natts as usize) * size_of::<CompactAttribute>()
}

/*
 * TupleDescSize(src): total size of the palloc'd TupleDesc block.
 *
 *   offsetof(struct TupleDescData, compact_attrs) +
 *   natts * sizeof(CompactAttribute) +
 *   natts * sizeof(FormData_pg_attribute)
 *
 * # Safety
 * `src` must point to a live TupleDescData.
 */
#[inline]
pub unsafe fn TupleDescSize(src: TupleDesc) -> usize {
    tupledesc_header_and_compact((*src).natts)
        + ((*src).natts as usize) * size_of::<FormData_pg_attribute>()
}

/*
 * Accessor for the i'th FormData_pg_attribute element of tupdesc.  This is the
 * merge of TupleDescAttrAddress + TupleDescAttr from tupdesc.h.
 *
 * # Safety
 * `tupdesc` must be live and `i` in 0..natts.
 */
#[inline]
pub unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: c_int) -> Form_pg_attribute {
    let base = tupdesc as *mut u8;
    let attrs = base.add(tupledesc_header_and_compact((*tupdesc).natts)) as Form_pg_attribute;
    attrs.add(i as usize)
}

/*
 * Accessor for the i'th CompactAttribute element of tupdesc.
 *
 * In Assert-enabled builds we first verify that the CompactAttribute is
 * correctly populated, exactly like the C static inline.
 *
 * # Safety
 * `tupdesc` must be live and `i` in 0..natts.
 */
#[inline]
pub unsafe fn TupleDescCompactAttr(tupdesc: TupleDesc, i: c_int) -> *mut CompactAttribute {
    let cattr = (&mut (*tupdesc).compact_attrs as *mut [CompactAttribute; FLEXIBLE_ARRAY_MEMBER]
        as *mut CompactAttribute)
        .add(i as usize);

    /* Check that the CompactAttribute is correctly populated */
    #[cfg(debug_assertions)]
    verify_compact_attribute(tupdesc, i);

    cattr
}

// ----------------------------------------------------------------------------
//   Local no-op / stub shims for not-yet-ported dependencies.
// ----------------------------------------------------------------------------

/*
 * catalog/catalog.c: IsCatalogRelationOid - true if the OID is that of a system
 * catalog relation.  Used by populate_compact_attribute only to decide whether a
 * not-null constraint should be recorded as VALID (catalogs) vs UNKNOWN.
 * Now backed by the real classifier in catalog/catalog.c.
 */
#[inline]
fn IsCatalogRelationOid(relid: Oid) -> bool {
    crate::catalog::catalog::IsCatalogRelationOid(relid)
}

/*
 * ResourceOwner shims (utils/resowner.c not yet ported).  The reference-count
 * arithmetic in IncrTupleDescRefCount/DecrTupleDescRefCount is real; only the
 * bookkeeping of the reference in CurrentResourceOwner is a no-op for now.
 *
 * TODO(pg-port): wire to the real ResourceOwner system (utils/resowner.c).
 */
#[inline]
fn ResourceOwnerEnlargeTupleDesc() {
    // TODO(pg-port): ResourceOwnerEnlarge(CurrentResourceOwner)
}
#[inline]
fn ResourceOwnerRememberTupleDesc(_tupdesc: TupleDesc) {
    // TODO(pg-port): ResourceOwnerRemember(CurrentResourceOwner, ..., &tupdesc_resowner_desc)
}
#[inline]
fn ResourceOwnerForgetTupleDesc(_tupdesc: TupleDesc) {
    // TODO(pg-port): ResourceOwnerForget(CurrentResourceOwner, ..., &tupdesc_resowner_desc)
}

// ----------------------------------------------------------------------------
//   tupdesc.c
// ----------------------------------------------------------------------------

/*
 * populate_compact_attribute_internal
 *		Helper function for populate_compact_attribute()
 *
 * # Safety
 * `src` and `dst` must be live.
 */
#[inline]
unsafe fn populate_compact_attribute_internal(src: Form_pg_attribute, dst: *mut CompactAttribute) {
    memset(dst as *mut c_void, 0, size_of::<CompactAttribute>());

    (*dst).attcacheoff = -1;
    (*dst).attlen = (*src).attlen;

    (*dst).attbyval = (*src).attbyval;
    (*dst).attispackable = (*src).attstorage != TYPSTORAGE_PLAIN;
    (*dst).atthasmissing = (*src).atthasmissing;
    (*dst).attisdropped = (*src).attisdropped;
    (*dst).attgenerated = (*src).attgenerated != b'\0' as c_char;

    /*
     * Assign nullability status for this column.  Assuming that a constraint
     * exists, at this point we don't know if a not-null constraint is valid,
     * so we assign UNKNOWN unless the table is a catalog, in which case we
     * know it's valid.
     */
    (*dst).attnullability = if !(*src).attnotnull {
        ATTNULLABLE_UNRESTRICTED
    } else if IsCatalogRelationOid((*src).attrelid) {
        ATTNULLABLE_VALID
    } else {
        ATTNULLABLE_UNKNOWN
    };

    match (*src).attalign {
        TYPALIGN_INT => {
            (*dst).attalignby = ALIGNOF_INT as uint8;
        }
        TYPALIGN_CHAR => {
            (*dst).attalignby = size_of::<c_char>() as uint8;
        }
        TYPALIGN_DOUBLE => {
            (*dst).attalignby = ALIGNOF_DOUBLE as uint8;
        }
        TYPALIGN_SHORT => {
            (*dst).attalignby = ALIGNOF_SHORT as uint8;
        }
        _ => {
            (*dst).attalignby = 0;
            elog!(ERROR, "invalid attalign value: {}", (*src).attalign as u8 as char);
        }
    }
}

/*
 * populate_compact_attribute
 *		Fill in the corresponding CompactAttribute element from the
 *		Form_pg_attribute for the given attribute number.  This must be called
 *		whenever a change is made to a Form_pg_attribute in the TupleDesc.
 *
 * # Safety
 * `tupdesc` must be live and `attnum` in 0..natts.
 */
pub unsafe fn populate_compact_attribute(tupdesc: TupleDesc, attnum: c_int) {
    let src = TupleDescAttr(tupdesc, attnum);

    /*
     * Don't use TupleDescCompactAttr to prevent infinite recursion in assert
     * builds.
     */
    let dst = (&mut (*tupdesc).compact_attrs as *mut [CompactAttribute; FLEXIBLE_ARRAY_MEMBER]
        as *mut CompactAttribute)
        .add(attnum as usize);

    populate_compact_attribute_internal(src, dst);
}

/*
 * verify_compact_attribute
 *		In Assert enabled builds, we verify that the CompactAttribute is
 *		populated correctly.  This helps find bugs in places such as ALTER
 *		TABLE where code makes changes to the FormData_pg_attribute but
 *		forgets to call populate_compact_attribute().
 *
 * This is used in TupleDescCompactAttr(), but declared here to allow access
 * to populate_compact_attribute_internal().
 *
 * # Safety
 * `tupdesc` must be live and `attnum` in 0..natts.
 */
pub unsafe fn verify_compact_attribute(tupdesc: TupleDesc, attnum: c_int) {
    #[cfg(debug_assertions)]
    {
        let mut cattr: CompactAttribute = core::mem::zeroed();
        let attr = TupleDescAttr(tupdesc, attnum);
        let mut tmp: CompactAttribute = core::mem::zeroed();

        /*
         * Make a temp copy of the TupleDesc's CompactAttribute.  This may be a
         * shared TupleDesc and the attcacheoff might get changed by another
         * backend.
         */
        let live = (&(*tupdesc).compact_attrs as *const [CompactAttribute; FLEXIBLE_ARRAY_MEMBER]
            as *const CompactAttribute)
            .add(attnum as usize);
        memcpy(
            &mut cattr as *mut CompactAttribute as *mut c_void,
            live as *const c_void,
            size_of::<CompactAttribute>(),
        );

        /*
         * Populate the temporary CompactAttribute from the corresponding
         * Form_pg_attribute
         */
        populate_compact_attribute_internal(attr, &mut tmp);

        /*
         * Make the attcacheoff match since it's been reset to -1 by
         * populate_compact_attribute_internal.  Same with attnullability.
         */
        tmp.attcacheoff = cattr.attcacheoff;
        tmp.attnullability = cattr.attnullability;

        /* Check the freshly populated CompactAttribute matches the TupleDesc's */
        Assert!(
            memcmp(
                &tmp as *const CompactAttribute as *const c_void,
                &cattr as *const CompactAttribute as *const c_void,
                size_of::<CompactAttribute>(),
            ) == 0
        );
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = (tupdesc, attnum);
    }
}

/*
 * CreateTemplateTupleDesc
 *		This function allocates an empty tuple descriptor structure.
 *
 * Tuple type ID information is initially set for an anonymous record type;
 * caller can overwrite this if needed.
 */
pub unsafe fn CreateTemplateTupleDesc(natts: c_int) -> TupleDesc {
    /*
     * sanity checks
     */
    Assert!(natts >= 0);

    /*
     * Allocate enough memory for the tuple descriptor, the CompactAttribute
     * array and also an array of FormData_pg_attribute.  See the layout note
     * on TupleDescData above.
     */
    let desc = palloc(
        offset_of!(TupleDescData, compact_attrs)
            + (natts as usize) * size_of::<CompactAttribute>()
            + (natts as usize) * size_of::<FormData_pg_attribute>(),
    ) as TupleDesc;

    /*
     * Initialize other fields of the tupdesc.
     */
    (*desc).natts = natts;
    (*desc).constr = null_mut();
    (*desc).tdtypeid = RECORDOID;
    (*desc).tdtypmod = -1;
    (*desc).tdrefcount = -1; /* assume not reference-counted */

    desc
}

/*
 * CreateTupleDesc
 *		This function allocates a new TupleDesc by copying a given
 *		Form_pg_attribute array.
 *
 * Tuple type ID information is initially set for an anonymous record type;
 * caller can overwrite this if needed.
 *
 * # Safety
 * `attrs` must point to `natts` valid Form_pg_attribute pointers.
 */
pub unsafe fn CreateTupleDesc(natts: c_int, attrs: *mut Form_pg_attribute) -> TupleDesc {
    let desc = CreateTemplateTupleDesc(natts);

    for i in 0..natts {
        memcpy(
            TupleDescAttr(desc, i) as *mut c_void,
            *attrs.add(i as usize) as *const c_void,
            ATTRIBUTE_FIXED_PART_SIZE,
        );
        populate_compact_attribute(desc, i);
    }
    desc
}

/*
 * CreateTupleDescCopy
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc.
 *
 * !!! Constraints and defaults are not copied !!!
 */
pub unsafe fn CreateTupleDescCopy(tupdesc: TupleDesc) -> TupleDesc {
    let desc = CreateTemplateTupleDesc((*tupdesc).natts);

    /* Flat-copy the attribute array */
    memcpy(
        TupleDescAttr(desc, 0) as *mut c_void,
        TupleDescAttr(tupdesc, 0) as *const c_void,
        (*desc).natts as usize * size_of::<FormData_pg_attribute>(),
    );

    /*
     * Since we're not copying constraints and defaults, clear fields
     * associated with them.
     */
    for i in 0..(*desc).natts {
        let att = TupleDescAttr(desc, i);

        (*att).attnotnull = false;
        (*att).atthasdef = false;
        (*att).atthasmissing = false;
        (*att).attidentity = b'\0' as c_char;
        (*att).attgenerated = b'\0' as c_char;

        populate_compact_attribute(desc, i);
    }

    /* We can copy the tuple type identification, too */
    (*desc).tdtypeid = (*tupdesc).tdtypeid;
    (*desc).tdtypmod = (*tupdesc).tdtypmod;

    desc
}

/*
 * CreateTupleDescTruncatedCopy
 *		This function creates a new TupleDesc with only the first 'natts'
 *		attributes from an existing TupleDesc
 *
 * !!! Constraints and defaults are not copied !!!
 */
pub unsafe fn CreateTupleDescTruncatedCopy(tupdesc: TupleDesc, natts: c_int) -> TupleDesc {
    Assert!(natts <= (*tupdesc).natts);

    let desc = CreateTemplateTupleDesc(natts);

    /* Flat-copy the attribute array */
    memcpy(
        TupleDescAttr(desc, 0) as *mut c_void,
        TupleDescAttr(tupdesc, 0) as *const c_void,
        (*desc).natts as usize * size_of::<FormData_pg_attribute>(),
    );

    /*
     * Since we're not copying constraints and defaults, clear fields
     * associated with them.
     */
    for i in 0..(*desc).natts {
        let att = TupleDescAttr(desc, i);

        (*att).attnotnull = false;
        (*att).atthasdef = false;
        (*att).atthasmissing = false;
        (*att).attidentity = b'\0' as c_char;
        (*att).attgenerated = b'\0' as c_char;

        populate_compact_attribute(desc, i);
    }

    /* We can copy the tuple type identification, too */
    (*desc).tdtypeid = (*tupdesc).tdtypeid;
    (*desc).tdtypmod = (*tupdesc).tdtypmod;

    desc
}

/*
 * CreateTupleDescCopyConstr
 *		This function creates a new TupleDesc by copying from an existing
 *		TupleDesc (including its constraints and defaults).
 */
pub unsafe fn CreateTupleDescCopyConstr(tupdesc: TupleDesc) -> TupleDesc {
    let constr = (*tupdesc).constr;

    let desc = CreateTemplateTupleDesc((*tupdesc).natts);

    /* Flat-copy the attribute array */
    memcpy(
        TupleDescAttr(desc, 0) as *mut c_void,
        TupleDescAttr(tupdesc, 0) as *const c_void,
        (*desc).natts as usize * size_of::<FormData_pg_attribute>(),
    );

    for i in 0..(*desc).natts {
        populate_compact_attribute(desc, i);

        (*TupleDescCompactAttr(desc, i)).attnullability =
            (*TupleDescCompactAttr(tupdesc, i)).attnullability;
    }

    /* Copy the TupleConstr data structure, if any */
    if !constr.is_null() {
        let cpy = palloc0(size_of::<TupleConstr>()) as *mut TupleConstr;

        (*cpy).has_not_null = (*constr).has_not_null;
        (*cpy).has_generated_stored = (*constr).has_generated_stored;
        (*cpy).has_generated_virtual = (*constr).has_generated_virtual;

        (*cpy).num_defval = (*constr).num_defval;
        if (*cpy).num_defval > 0 {
            (*cpy).defval =
                palloc((*cpy).num_defval as usize * size_of::<AttrDefault>()) as *mut AttrDefault;
            memcpy(
                (*cpy).defval as *mut c_void,
                (*constr).defval as *const c_void,
                (*cpy).num_defval as usize * size_of::<AttrDefault>(),
            );
            let mut i = (*cpy).num_defval as i32 - 1;
            while i >= 0 {
                (*(*cpy).defval.add(i as usize)).adbin =
                    pstrdup((*(*constr).defval.add(i as usize)).adbin);
                i -= 1;
            }
        }

        if !(*constr).missing.is_null() {
            (*cpy).missing =
                palloc((*tupdesc).natts as usize * size_of::<AttrMissing>()) as *mut AttrMissing;
            memcpy(
                (*cpy).missing as *mut c_void,
                (*constr).missing as *const c_void,
                (*tupdesc).natts as usize * size_of::<AttrMissing>(),
            );
            let mut i = (*tupdesc).natts - 1;
            while i >= 0 {
                if (*(*constr).missing.add(i as usize)).am_present {
                    let attr = TupleDescCompactAttr(tupdesc, i);

                    (*(*cpy).missing.add(i as usize)).am_value = datumCopy(
                        (*(*constr).missing.add(i as usize)).am_value,
                        (*attr).attbyval,
                        (*attr).attlen as c_int,
                    );
                }
                i -= 1;
            }
        }

        (*cpy).num_check = (*constr).num_check;
        if (*cpy).num_check > 0 {
            (*cpy).check =
                palloc((*cpy).num_check as usize * size_of::<ConstrCheck>()) as *mut ConstrCheck;
            memcpy(
                (*cpy).check as *mut c_void,
                (*constr).check as *const c_void,
                (*cpy).num_check as usize * size_of::<ConstrCheck>(),
            );
            let mut i = (*cpy).num_check as i32 - 1;
            while i >= 0 {
                let dst = (*cpy).check.add(i as usize);
                let srcc = (*constr).check.add(i as usize);
                (*dst).ccname = pstrdup((*srcc).ccname);
                (*dst).ccbin = pstrdup((*srcc).ccbin);
                (*dst).ccenforced = (*srcc).ccenforced;
                (*dst).ccvalid = (*srcc).ccvalid;
                (*dst).ccnoinherit = (*srcc).ccnoinherit;
                i -= 1;
            }
        }

        (*desc).constr = cpy;
    }

    /* We can copy the tuple type identification, too */
    (*desc).tdtypeid = (*tupdesc).tdtypeid;
    (*desc).tdtypmod = (*tupdesc).tdtypmod;

    desc
}

/*
 * TupleDescCopy
 *		Copy a tuple descriptor into caller-supplied memory.
 *		The memory may be shared memory mapped at any address, and must
 *		be sufficient to hold TupleDescSize(src) bytes.
 *
 * !!! Constraints and defaults are not copied !!!
 */
pub unsafe fn TupleDescCopy(dst: TupleDesc, src: TupleDesc) {
    /* Flat-copy the header and attribute arrays */
    memcpy(dst as *mut c_void, src as *const c_void, TupleDescSize(src));

    /*
     * Since we're not copying constraints and defaults, clear fields
     * associated with them.
     */
    for i in 0..(*dst).natts {
        let att = TupleDescAttr(dst, i);

        (*att).attnotnull = false;
        (*att).atthasdef = false;
        (*att).atthasmissing = false;
        (*att).attidentity = b'\0' as c_char;
        (*att).attgenerated = b'\0' as c_char;

        populate_compact_attribute(dst, i);
    }
    (*dst).constr = null_mut();

    /*
     * Also, assume the destination is not to be ref-counted.  (Copying the
     * source's refcount would be wrong in any case.)
     */
    (*dst).tdrefcount = -1;
}

/*
 * TupleDescCopyEntry
 *		This function copies a single attribute structure from one tuple
 *		descriptor to another.
 *
 * !!! Constraints and defaults are not copied !!!
 */
pub unsafe fn TupleDescCopyEntry(
    dst: TupleDesc,
    dstAttno: AttrNumber,
    src: TupleDesc,
    srcAttno: AttrNumber,
) {
    let dstAtt = TupleDescAttr(dst, dstAttno as c_int - 1);
    let srcAtt = TupleDescAttr(src, srcAttno as c_int - 1);

    /*
     * sanity checks
     */
    Assert!(PointerIsValid(src));
    Assert!(PointerIsValid(dst));
    Assert!(srcAttno >= 1);
    Assert!((srcAttno as c_int) <= (*src).natts);
    Assert!(dstAttno >= 1);
    Assert!((dstAttno as c_int) <= (*dst).natts);

    memcpy(
        dstAtt as *mut c_void,
        srcAtt as *const c_void,
        ATTRIBUTE_FIXED_PART_SIZE,
    );

    (*dstAtt).attnum = dstAttno;

    /* since we're not copying constraints or defaults, clear these */
    (*dstAtt).attnotnull = false;
    (*dstAtt).atthasdef = false;
    (*dstAtt).atthasmissing = false;
    (*dstAtt).attidentity = b'\0' as c_char;
    (*dstAtt).attgenerated = b'\0' as c_char;

    populate_compact_attribute(dst, dstAttno as c_int - 1);
}

/*
 * Free a TupleDesc including all substructure
 */
pub unsafe fn FreeTupleDesc(tupdesc: TupleDesc) {
    /*
     * Possibly this should assert tdrefcount == 0, to disallow explicit
     * freeing of un-refcounted tupdescs?
     */
    Assert!((*tupdesc).tdrefcount <= 0);

    if !(*tupdesc).constr.is_null() {
        let constr = (*tupdesc).constr;
        if (*constr).num_defval > 0 {
            let attrdef = (*constr).defval;

            let mut i = (*constr).num_defval as i32 - 1;
            while i >= 0 {
                pfree((*attrdef.add(i as usize)).adbin as *mut c_void);
                i -= 1;
            }
            pfree(attrdef as *mut c_void);
        }
        if !(*constr).missing.is_null() {
            let attrmiss = (*constr).missing;

            let mut i = (*tupdesc).natts - 1;
            while i >= 0 {
                if (*attrmiss.add(i as usize)).am_present
                    && !(*TupleDescAttr(tupdesc, i)).attbyval
                {
                    pfree(DatumGetPointer((*attrmiss.add(i as usize)).am_value) as *mut c_void);
                }
                i -= 1;
            }
            pfree(attrmiss as *mut c_void);
        }
        if (*constr).num_check > 0 {
            let check = (*constr).check;

            let mut i = (*constr).num_check as i32 - 1;
            while i >= 0 {
                pfree((*check.add(i as usize)).ccname as *mut c_void);
                pfree((*check.add(i as usize)).ccbin as *mut c_void);
                i -= 1;
            }
            pfree(check as *mut c_void);
        }
        pfree(constr as *mut c_void);
    }

    pfree(tupdesc as *mut c_void);
}

/*
 * Increment the reference count of a tupdesc, and log the reference in
 * CurrentResourceOwner.
 *
 * Do not apply this to tupdescs that are not being refcounted.  (Use the
 * macro PinTupleDesc for tupdescs of uncertain status.)
 */
pub unsafe fn IncrTupleDescRefCount(tupdesc: TupleDesc) {
    Assert!((*tupdesc).tdrefcount >= 0);

    ResourceOwnerEnlargeTupleDesc();
    (*tupdesc).tdrefcount += 1;
    ResourceOwnerRememberTupleDesc(tupdesc);
}

/*
 * Decrement the reference count of a tupdesc, remove the corresponding
 * reference from CurrentResourceOwner, and free the tupdesc if no more
 * references remain.
 *
 * Do not apply this to tupdescs that are not being refcounted.  (Use the
 * macro ReleaseTupleDesc for tupdescs of uncertain status.)
 */
pub unsafe fn DecrTupleDescRefCount(tupdesc: TupleDesc) {
    Assert!((*tupdesc).tdrefcount > 0);

    ResourceOwnerForgetTupleDesc(tupdesc);
    (*tupdesc).tdrefcount -= 1;
    if (*tupdesc).tdrefcount == 0 {
        FreeTupleDesc(tupdesc);
    }
}

/*
 * PinTupleDesc(tupdesc) - increment refcount of a tupdesc of uncertain status.
 */
pub unsafe fn PinTupleDesc(tupdesc: TupleDesc) {
    if (*tupdesc).tdrefcount >= 0 {
        IncrTupleDescRefCount(tupdesc);
    }
}

/*
 * ReleaseTupleDesc(tupdesc) - decrement refcount of a tupdesc of uncertain status.
 */
pub unsafe fn ReleaseTupleDesc(tupdesc: TupleDesc) {
    if (*tupdesc).tdrefcount >= 0 {
        DecrTupleDescRefCount(tupdesc);
    }
}

/*
 * Compare two TupleDesc structures for logical equality
 */
pub unsafe fn equalTupleDescs(tupdesc1: TupleDesc, tupdesc2: TupleDesc) -> bool {
    if (*tupdesc1).natts != (*tupdesc2).natts {
        return false;
    }
    if (*tupdesc1).tdtypeid != (*tupdesc2).tdtypeid {
        return false;
    }

    /* tdtypmod and tdrefcount are not checked */

    for i in 0..(*tupdesc1).natts {
        let attr1 = TupleDescAttr(tupdesc1, i);
        let attr2 = TupleDescAttr(tupdesc2, i);

        /*
         * We do not need to check every single field here: we can disregard
         * attrelid and attnum (which were used to place the row in the attrs
         * array in the first place).  It might look like we could dispense
         * with checking attlen/attbyval/attalign, since these are derived
         * from atttypid; but in the case of dropped columns we must check
         * them (since atttypid will be zero for all dropped columns) and in
         * general it seems safer to check them always.
         *
         * We intentionally ignore atthasmissing, since that's not very
         * relevant in tupdescs, which lack the attmissingval field.
         */
        if strcmp(NameStr(&(*attr1).attname), NameStr(&(*attr2).attname)) != 0 {
            return false;
        }
        if (*attr1).atttypid != (*attr2).atttypid {
            return false;
        }
        if (*attr1).attlen != (*attr2).attlen {
            return false;
        }
        if (*attr1).attndims != (*attr2).attndims {
            return false;
        }
        if (*attr1).atttypmod != (*attr2).atttypmod {
            return false;
        }
        if (*attr1).attbyval != (*attr2).attbyval {
            return false;
        }
        if (*attr1).attalign != (*attr2).attalign {
            return false;
        }
        if (*attr1).attstorage != (*attr2).attstorage {
            return false;
        }
        if (*attr1).attcompression != (*attr2).attcompression {
            return false;
        }
        if (*attr1).attnotnull != (*attr2).attnotnull {
            return false;
        }

        /*
         * When the column has a not-null constraint, we also need to consider
         * its validity aspect, which only manifests in CompactAttribute->
         * attnullability, so verify that.
         */
        if (*attr1).attnotnull {
            let cattr1 = TupleDescCompactAttr(tupdesc1, i);
            let cattr2 = TupleDescCompactAttr(tupdesc2, i);

            Assert!((*cattr1).attnullability != ATTNULLABLE_UNKNOWN);
            Assert!(
                ((*cattr1).attnullability == ATTNULLABLE_UNKNOWN)
                    == ((*cattr2).attnullability == ATTNULLABLE_UNKNOWN)
            );

            if (*cattr1).attnullability != (*cattr2).attnullability {
                return false;
            }
        }
        if (*attr1).atthasdef != (*attr2).atthasdef {
            return false;
        }
        if (*attr1).attidentity != (*attr2).attidentity {
            return false;
        }
        if (*attr1).attgenerated != (*attr2).attgenerated {
            return false;
        }
        if (*attr1).attisdropped != (*attr2).attisdropped {
            return false;
        }
        if (*attr1).attislocal != (*attr2).attislocal {
            return false;
        }
        if (*attr1).attinhcount != (*attr2).attinhcount {
            return false;
        }
        if (*attr1).attcollation != (*attr2).attcollation {
            return false;
        }
        /* variable-length fields are not even present... */
    }

    if !(*tupdesc1).constr.is_null() {
        let constr1 = (*tupdesc1).constr;
        let constr2 = (*tupdesc2).constr;

        if constr2.is_null() {
            return false;
        }
        if (*constr1).has_not_null != (*constr2).has_not_null {
            return false;
        }
        if (*constr1).has_generated_stored != (*constr2).has_generated_stored {
            return false;
        }
        if (*constr1).has_generated_virtual != (*constr2).has_generated_virtual {
            return false;
        }
        let n = (*constr1).num_defval as c_int;
        if n != (*constr2).num_defval as c_int {
            return false;
        }
        /* We assume here that both AttrDefault arrays are in adnum order */
        for i in 0..n {
            let defval1 = (*constr1).defval.add(i as usize);
            let defval2 = (*constr2).defval.add(i as usize);

            if (*defval1).adnum != (*defval2).adnum {
                return false;
            }
            if strcmp((*defval1).adbin, (*defval2).adbin) != 0 {
                return false;
            }
        }
        if !(*constr1).missing.is_null() {
            if (*constr2).missing.is_null() {
                return false;
            }
            for i in 0..(*tupdesc1).natts {
                let missval1 = (*constr1).missing.add(i as usize);
                let missval2 = (*constr2).missing.add(i as usize);

                if (*missval1).am_present != (*missval2).am_present {
                    return false;
                }
                if (*missval1).am_present {
                    let missatt1 = TupleDescCompactAttr(tupdesc1, i);

                    if !datumIsEqual(
                        (*missval1).am_value,
                        (*missval2).am_value,
                        (*missatt1).attbyval,
                        (*missatt1).attlen as c_int,
                    ) {
                        return false;
                    }
                }
            }
        } else if !(*constr2).missing.is_null() {
            return false;
        }
        let n = (*constr1).num_check as c_int;
        if n != (*constr2).num_check as c_int {
            return false;
        }

        /*
         * Similarly, we rely here on the ConstrCheck entries being sorted by
         * name.  If there are duplicate names, the outcome of the comparison
         * is uncertain, but that should not happen.
         */
        for i in 0..n {
            let check1 = (*constr1).check.add(i as usize);
            let check2 = (*constr2).check.add(i as usize);

            if !(strcmp((*check1).ccname, (*check2).ccname) == 0
                && strcmp((*check1).ccbin, (*check2).ccbin) == 0
                && (*check1).ccenforced == (*check2).ccenforced
                && (*check1).ccvalid == (*check2).ccvalid
                && (*check1).ccnoinherit == (*check2).ccnoinherit)
            {
                return false;
            }
        }
    } else if !(*tupdesc2).constr.is_null() {
        return false;
    }
    true
}

/*
 * equalRowTypes
 *
 * This determines whether two tuple descriptors have equal row types.  This
 * only checks those fields in pg_attribute that are applicable for row types,
 * while ignoring those fields that define the physical row storage or those
 * that define table column metadata.
 *
 * Specifically, this checks:
 *
 * - same number of attributes
 * - same composite type ID (but could both be zero)
 * - corresponding attributes (in order) have same the name, type, typmod,
 *   collation
 *
 * This is used to check whether two record types are compatible, whether
 * function return row types are the same, and other similar situations.
 *
 * Note: We deliberately do not check the tdtypmod field.  This allows
 * typcache.c to use this routine to see if a cached record type matches a
 * requested type.
 */
pub unsafe fn equalRowTypes(tupdesc1: TupleDesc, tupdesc2: TupleDesc) -> bool {
    if (*tupdesc1).natts != (*tupdesc2).natts {
        return false;
    }
    if (*tupdesc1).tdtypeid != (*tupdesc2).tdtypeid {
        return false;
    }

    for i in 0..(*tupdesc1).natts {
        let attr1 = TupleDescAttr(tupdesc1, i);
        let attr2 = TupleDescAttr(tupdesc2, i);

        if strcmp(NameStr(&(*attr1).attname), NameStr(&(*attr2).attname)) != 0 {
            return false;
        }
        if (*attr1).atttypid != (*attr2).atttypid {
            return false;
        }
        if (*attr1).atttypmod != (*attr2).atttypmod {
            return false;
        }
        if (*attr1).attcollation != (*attr2).attcollation {
            return false;
        }

        /* Record types derived from tables could have dropped fields. */
        if (*attr1).attisdropped != (*attr2).attisdropped {
            return false;
        }
    }

    true
}

/*
 * hashRowType
 *
 * If two tuple descriptors would be considered equal by equalRowTypes()
 * then their hash value will be equal according to this function.
 */
pub unsafe fn hashRowType(desc: TupleDesc) -> uint32 {
    let mut s: uint32;

    s = hash_combine(0, hash_uint32((*desc).natts as uint32) as uint32);
    s = hash_combine(s, hash_uint32((*desc).tdtypeid) as uint32);
    for i in 0..(*desc).natts {
        s = hash_combine(s, hash_uint32((*TupleDescAttr(desc, i)).atttypid) as uint32);
    }

    s
}

/*
 * TupleDescInitEntry
 *		This function initializes a single attribute structure in
 *		a previously allocated tuple descriptor.
 *
 * If attributeName is NULL, the attname field is set to an empty string
 * (this is for cases where we don't know or need a name for the field).
 * Also, some callers use this function to change the datatype-related fields
 * in an existing tupdesc; they pass attributeName = NameStr(att->attname)
 * to indicate that the attname field shouldn't be modified.
 *
 * Note that attcollation is set to the default for the specified datatype.
 * If a nondefault collation is needed, insert it afterwards using
 * TupleDescInitEntryCollation.
 *
 * STUBBED: the datatype-info fetch goes through SearchSysCache1(TYPEOID, ...)
 * to fill attlen/attbyval/attalign/attstorage/attcollation; the syscache is not
 * yet ported.  The attribute-field scaffolding (everything before the syscache
 * lookup) is preserved verbatim as a comment along with the original C body.
 *
 * TODO(pg-port): needs utils/syscache.c (SearchSysCache1) + access/htup_details.h
 * (GETSTRUCT) + catalog/pg_type Form_pg_type.
 */
pub unsafe fn TupleDescInitEntry(
    desc: TupleDesc,
    attributeNumber: AttrNumber,
    attributeName: *const c_char,
    oidtypeid: Oid,
    typmod: int32,
    attdim: c_int,
) {
    let _ = (desc, attributeNumber, attributeName, oidtypeid, typmod, attdim);

    // C body:
    //   HeapTuple   tuple;
    //   Form_pg_type typeForm;
    //   Form_pg_attribute att;
    //
    //   Assert(PointerIsValid(desc));
    //   Assert(attributeNumber >= 1);
    //   Assert(attributeNumber <= desc->natts);
    //   Assert(attdim >= 0);
    //   Assert(attdim <= PG_INT16_MAX);
    //
    //   att = TupleDescAttr(desc, attributeNumber - 1);
    //   att->attrelid = 0;          /* dummy value */
    //
    //   if (attributeName == NULL)
    //       MemSet(NameStr(att->attname), 0, NAMEDATALEN);
    //   else if (attributeName != NameStr(att->attname))
    //       namestrcpy(&(att->attname), attributeName);
    //
    //   att->atttypmod = typmod;
    //   att->attnum = attributeNumber;
    //   att->attndims = attdim;
    //   att->attnotnull = false;
    //   att->atthasdef = false;
    //   att->atthasmissing = false;
    //   att->attidentity = '\0';
    //   att->attgenerated = '\0';
    //   att->attisdropped = false;
    //   att->attislocal = true;
    //   att->attinhcount = 0;
    //
    //   tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oidtypeid));
    //   if (!HeapTupleIsValid(tuple))
    //       elog(ERROR, "cache lookup failed for type %u", oidtypeid);
    //   typeForm = (Form_pg_type) GETSTRUCT(tuple);
    //
    //   att->atttypid = oidtypeid;
    //   att->attlen = typeForm->typlen;
    //   att->attbyval = typeForm->typbyval;
    //   att->attalign = typeForm->typalign;
    //   att->attstorage = typeForm->typstorage;
    //   att->attcompression = InvalidCompressionMethod;
    //   att->attcollation = typeForm->typcollation;
    //
    //   populate_compact_attribute(desc, attributeNumber - 1);
    //   ReleaseSysCache(tuple);
    unimplemented!("TupleDescInitEntry: needs utils/syscache.c (SearchSysCache1 for TYPEOID)")
}

/*
 * TupleDescInitBuiltinEntry
 *		Initialize a tuple descriptor without catalog access.  Only
 *		a limited range of builtin types are supported.
 */
pub unsafe fn TupleDescInitBuiltinEntry(
    desc: TupleDesc,
    attributeNumber: AttrNumber,
    attributeName: *const c_char,
    oidtypeid: Oid,
    typmod: int32,
    attdim: c_int,
) {
    /* sanity checks */
    Assert!(PointerIsValid(desc));
    Assert!(attributeNumber >= 1);
    Assert!((attributeNumber as c_int) <= (*desc).natts);
    Assert!(attdim >= 0);
    Assert!(attdim <= PG_INT16_MAX as c_int);

    /* initialize the attribute fields */
    let att = TupleDescAttr(desc, attributeNumber as c_int - 1);
    (*att).attrelid = 0; /* dummy value */

    /* unlike TupleDescInitEntry, we require an attribute name */
    Assert!(PointerIsValid(attributeName));
    namestrcpy(&mut (*att).attname, attributeName);

    (*att).atttypmod = typmod;

    (*att).attnum = attributeNumber;
    (*att).attndims = attdim as int16;

    (*att).attnotnull = false;
    (*att).atthasdef = false;
    (*att).atthasmissing = false;
    (*att).attidentity = b'\0' as c_char;
    (*att).attgenerated = b'\0' as c_char;
    (*att).attisdropped = false;
    (*att).attislocal = true;
    (*att).attinhcount = 0;
    /* variable-length fields are not present in tupledescs */

    (*att).atttypid = oidtypeid;

    /*
     * Our goal here is to support just enough types to let basic builtin
     * commands work without catalog access - e.g. so that we can do certain
     * things even in processes that are not connected to a database.
     */
    match oidtypeid {
        TEXTOID | TEXTARRAYOID => {
            (*att).attlen = -1;
            (*att).attbyval = false;
            (*att).attalign = TYPALIGN_INT;
            (*att).attstorage = TYPSTORAGE_EXTENDED;
            (*att).attcompression = InvalidCompressionMethod;
            (*att).attcollation = DEFAULT_COLLATION_OID;
        }

        BOOLOID => {
            (*att).attlen = 1;
            (*att).attbyval = true;
            (*att).attalign = TYPALIGN_CHAR;
            (*att).attstorage = TYPSTORAGE_PLAIN;
            (*att).attcompression = InvalidCompressionMethod;
            (*att).attcollation = InvalidOid;
        }

        INT4OID => {
            (*att).attlen = 4;
            (*att).attbyval = true;
            (*att).attalign = TYPALIGN_INT;
            (*att).attstorage = TYPSTORAGE_PLAIN;
            (*att).attcompression = InvalidCompressionMethod;
            (*att).attcollation = InvalidOid;
        }

        INT8OID => {
            (*att).attlen = 8;
            (*att).attbyval = FLOAT8PASSBYVAL;
            (*att).attalign = TYPALIGN_DOUBLE;
            (*att).attstorage = TYPSTORAGE_PLAIN;
            (*att).attcompression = InvalidCompressionMethod;
            (*att).attcollation = InvalidOid;
        }

        OIDOID => {
            (*att).attlen = 4;
            (*att).attbyval = true;
            (*att).attalign = TYPALIGN_INT;
            (*att).attstorage = TYPSTORAGE_PLAIN;
            (*att).attcompression = InvalidCompressionMethod;
            (*att).attcollation = InvalidOid;
        }

        _ => {
            elog!(ERROR, "unsupported type {}", oidtypeid);
        }
    }

    populate_compact_attribute(desc, attributeNumber as c_int - 1);
}

/*
 * TupleDescInitEntryCollation
 *
 * Assign a nondefault collation to a previously initialized tuple descriptor
 * entry.
 */
pub unsafe fn TupleDescInitEntryCollation(
    desc: TupleDesc,
    attributeNumber: AttrNumber,
    collationid: Oid,
) {
    /*
     * sanity checks
     */
    Assert!(PointerIsValid(desc));
    Assert!(attributeNumber >= 1);
    Assert!((attributeNumber as c_int) <= (*desc).natts);

    (*TupleDescAttr(desc, attributeNumber as c_int - 1)).attcollation = collationid;
}

/*
 * BuildDescFromLists
 *
 * Build a TupleDesc given lists of column names (as String nodes),
 * column type OIDs, typmods, and collation OIDs.
 *
 * No constraints are generated.
 *
 * This is for use with functions returning RECORD.
 *
 * STUBBED: iterates four parallel Lists via forfour and calls TupleDescInitEntry
 * (itself stubbed on the syscache).  list machinery + TupleDescInitEntry are not
 * available, so this is left unimplemented.
 *
 * TODO(pg-port): needs nodes/pg_list.c (forfour/list_length/lfirst_*) and a real
 * TupleDescInitEntry.
 */
pub unsafe fn BuildDescFromLists(
    names: *const List,
    types: *const List,
    typmods: *const List,
    collations: *const List,
) -> TupleDesc {
    let _ = (names, types, typmods, collations);

    // C body:
    //   int         natts;
    //   AttrNumber  attnum;
    //   ListCell   *l1, *l2, *l3, *l4;
    //   TupleDesc   desc;
    //
    //   natts = list_length(names);
    //   Assert(natts == list_length(types));
    //   Assert(natts == list_length(typmods));
    //   Assert(natts == list_length(collations));
    //
    //   desc = CreateTemplateTupleDesc(natts);
    //
    //   attnum = 0;
    //   forfour(l1, names, l2, types, l3, typmods, l4, collations)
    //   {
    //       char       *attname = strVal(lfirst(l1));
    //       Oid         atttypid = lfirst_oid(l2);
    //       int32       atttypmod = lfirst_int(l3);
    //       Oid         attcollation = lfirst_oid(l4);
    //
    //       attnum++;
    //       TupleDescInitEntry(desc, attnum, attname, atttypid, atttypmod, 0);
    //       TupleDescInitEntryCollation(desc, attnum, attcollation);
    //   }
    //
    //   return desc;
    unimplemented!("BuildDescFromLists: needs nodes/pg_list (forfour) + TupleDescInitEntry")
}

/*
 * Get default expression (or NULL if none) for the given attribute number.
 *
 * STUBBED: the lookup over constr->defval is real, but turning an adbin string
 * back into a Node requires nodeToString's inverse stringToNode (readfuncs.c),
 * which is not yet ported.  Left unimplemented as a whole rather than half-done.
 *
 * TODO(pg-port): needs nodes/read.c (stringToNode) to deserialize adbin.
 */
pub unsafe fn TupleDescGetDefault(tupdesc: TupleDesc, attnum: AttrNumber) -> *mut Node {
    let _ = (tupdesc, attnum);

    // C body:
    //   Node       *result = NULL;
    //   if (tupdesc->constr)
    //   {
    //       AttrDefault *attrdef = tupdesc->constr->defval;
    //       for (int i = 0; i < tupdesc->constr->num_defval; i++)
    //       {
    //           if (attrdef[i].adnum == attnum)
    //           {
    //               result = stringToNode(attrdef[i].adbin);
    //               break;
    //           }
    //       }
    //   }
    //   return result;
    unimplemented!("TupleDescGetDefault: needs nodes/read.c (stringToNode) to deserialize adbin")
}

// ----------------------------------------------------------------------------
//   ResourceOwner callbacks (utils/resowner.c not yet ported) - kept as stubs.
// ----------------------------------------------------------------------------

/*
 * static void ResOwnerReleaseTupleDesc(Datum res)
 *
 * TODO(pg-port): registered in tupdesc_resowner_desc once resowner.c is ported.
 */
#[allow(dead_code)]
unsafe fn ResOwnerReleaseTupleDesc(res: Datum) {
    let tupdesc = DatumGetPointer(res) as TupleDesc;

    /* Like DecrTupleDescRefCount, but don't call ResourceOwnerForget() */
    Assert!((*tupdesc).tdrefcount > 0);
    (*tupdesc).tdrefcount -= 1;
    if (*tupdesc).tdrefcount == 0 {
        FreeTupleDesc(tupdesc);
    }
}

/*
 * static char *ResOwnerPrintTupleDesc(Datum res)
 *
 * TODO(pg-port): needs psprintf (utils/mmgr/mcxt.c) - stubbed.
 */
#[allow(dead_code)]
unsafe fn ResOwnerPrintTupleDesc(res: Datum) -> *mut c_char {
    let _ = res;
    // C body:
    //   TupleDesc tupdesc = (TupleDesc) DatumGetPointer(res);
    //   return psprintf("TupleDesc %p (%u,%d)",
    //                   tupdesc, tupdesc->tdtypeid, tupdesc->tdtypmod);
    unimplemented!("ResOwnerPrintTupleDesc: needs psprintf (utils/mmgr/mcxt.c)")
}

#[cfg(test)]
mod tests {
    use super::*;

    /* sizeof(CompactAttribute) must be exactly 16 bytes. */
    #[test]
    fn compact_attribute_is_16_bytes() {
        assert_eq!(size_of::<CompactAttribute>(), 16);
    }

    /*
     * The compact_attrs flexible array must be 8-aligned within the header, and
     * the TupleDescAttr arithmetic must land the Form array at
     * offset_of!(compact_attrs) + natts*16.
     */
    #[test]
    fn tupledesc_layout_arithmetic() {
        let off = offset_of!(TupleDescData, compact_attrs);
        assert_eq!(off % 8, 0, "compact_attrs must be 8-aligned");

        for natts in [0i32, 1, 2, 5] {
            let expect_form_off = off + natts as usize * size_of::<CompactAttribute>();
            assert_eq!(
                tupledesc_header_and_compact(natts),
                expect_form_off,
                "Form array offset for natts={}",
                natts
            );
        }

        unsafe {
            // Build a 2-column descriptor and verify the Form pointer for column 0
            // sits exactly at base + offset_of!(compact_attrs) + 2*16.
            let td = CreateTemplateTupleDesc(2);
            let base = td as *mut u8;
            let form0 = TupleDescAttr(td, 0) as *mut u8;
            let expect = base.add(off + 2 * size_of::<CompactAttribute>());
            assert_eq!(form0, expect);
            // The two Form entries are sizeof(FormData_pg_attribute) apart.
            let form1 = TupleDescAttr(td, 1) as *mut u8;
            assert_eq!(
                form1.offset_from(form0) as usize,
                size_of::<FormData_pg_attribute>()
            );
            FreeTupleDesc(td);
        }
    }

    /* TupleDescSize matches the sum the allocator uses. */
    #[test]
    fn tupledesc_size_matches_alloc() {
        unsafe {
            let td = CreateTemplateTupleDesc(3);
            let off = offset_of!(TupleDescData, compact_attrs);
            let expect = off
                + 3 * size_of::<CompactAttribute>()
                + 3 * size_of::<FormData_pg_attribute>();
            assert_eq!(TupleDescSize(td), expect);
            FreeTupleDesc(td);
        }
    }

    /*
     * End-to-end exercise of the real path: build a 2-column TupleDesc with the
     * builtin entry initializer, check both the Form and Compact mirrors, then
     * copy and compare.
     */
    #[test]
    fn builtin_entries_copy_equal_and_hash_stable() {
        unsafe {
            let td = CreateTemplateTupleDesc(2);
            TupleDescInitBuiltinEntry(td, 1, c"a".as_ptr(), INT4OID, -1, 0);
            TupleDescInitBuiltinEntry(td, 2, c"b".as_ptr(), TEXTOID, -1, 0);

            // Column 0: INT4OID "a", attlen 4, by-value, int-aligned.
            let a = TupleDescAttr(td, 0);
            assert_eq!(strcmp(NameStr(&(*a).attname), c"a".as_ptr()), 0);
            assert_eq!((*a).atttypid, INT4OID);
            assert_eq!((*a).attlen, 4);
            assert!((*a).attbyval);
            let ca = TupleDescCompactAttr(td, 0);
            assert_eq!((*ca).attlen, 4);
            assert!((*ca).attbyval);
            assert_eq!((*ca).attalignby, ALIGNOF_INT as uint8);
            assert_eq!((*ca).attcacheoff, -1);
            // INT4 is plain storage -> not packable.
            assert!(!(*ca).attispackable);

            // Column 1: TEXTOID "b", varlena (attlen -1), not by-value, packable.
            let b = TupleDescAttr(td, 1);
            assert_eq!(strcmp(NameStr(&(*b).attname), c"b".as_ptr()), 0);
            assert_eq!((*b).atttypid, TEXTOID);
            assert_eq!((*b).attlen, -1);
            assert!(!(*b).attbyval);
            let cb = TupleDescCompactAttr(td, 1);
            assert_eq!((*cb).attlen, -1);
            assert!(!(*cb).attbyval);
            // TEXT uses 'i' alignment and EXTENDED storage.
            assert_eq!((*cb).attalignby, ALIGNOF_INT as uint8);
            assert!((*cb).attispackable);

            // Copy and compare for logical equality.
            let copy = CreateTupleDescCopy(td);
            assert!(equalTupleDescs(td, copy));
            assert!(equalRowTypes(td, copy));

            // hashRowType is stable and equal for equal row types.
            let h1 = hashRowType(td);
            let h2 = hashRowType(copy);
            assert_eq!(h1, h2);
            // Re-hashing the same descriptor is deterministic.
            assert_eq!(hashRowType(td), h1);

            // A descriptor differing in a column type is not equal.
            let other = CreateTemplateTupleDesc(2);
            TupleDescInitBuiltinEntry(other, 1, c"a".as_ptr(), INT8OID, -1, 0);
            TupleDescInitBuiltinEntry(other, 2, c"b".as_ptr(), TEXTOID, -1, 0);
            assert!(!equalTupleDescs(td, other));

            FreeTupleDesc(other);
            FreeTupleDesc(copy);
            FreeTupleDesc(td);
        }
    }

    /* TupleDescCopyEntry copies the fixed part and resets constraint flags. */
    #[test]
    fn copy_entry_moves_column() {
        unsafe {
            let src = CreateTemplateTupleDesc(1);
            TupleDescInitBuiltinEntry(src, 1, c"x".as_ptr(), OIDOID, -1, 0);

            let dst = CreateTemplateTupleDesc(2);
            // dst columns start zeroed; copy src col 1 into dst col 2.
            TupleDescCopyEntry(dst, 2, src, 1);

            let d = TupleDescAttr(dst, 1);
            assert_eq!(strcmp(NameStr(&(*d).attname), c"x".as_ptr()), 0);
            assert_eq!((*d).atttypid, OIDOID);
            assert_eq!((*d).attnum, 2); // renumbered to dst position
            assert!(!(*d).attnotnull);
            let cd = TupleDescCompactAttr(dst, 1);
            assert_eq!((*cd).attlen, 4);
            assert!((*cd).attbyval);

            FreeTupleDesc(dst);
            FreeTupleDesc(src);
        }
    }
}
