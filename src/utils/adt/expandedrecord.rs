//! Translation of postgres/src/backend/utils/adt/expandedrecord.c
//! (merged with its header postgres/src/include/utils/expandedrecord.h).
//!
//! Functions for manipulating composite expanded objects.
//!
//! This module supports "expanded objects" (cf. expandeddatum.h) that can
//! store values of named composite types, domains over named composite types,
//! and record types (registered or anonymous).
//!
//! #include mapping:
//!   - "postgres.h"            -> crate::prelude::*
//!   - "access/detoast.h"      -> crate::access::common::detoast (detoast_external_attr)
//!   - "access/heaptoast.h"    -> crate::access::heap::heaptoast (toast_flatten_tuple)
//!   - "access/htup_details.h" -> crate::access::htup_details
//!   - "catalog/heap.h"        -> crate::catalog::heap (SystemAttributeByName)
//!   - "catalog/pg_type.h"     -> crate::catalog::pg_type (TYPTYPE_DOMAIN)
//!   - "utils/builtins.h"      -> crate::utils::adt::format_type (format_type_be)
//!   - "utils/datum.h"         -> crate::utils::adt::datum (datumCopy)
//!   - "utils/expandedrecord.h"-> THIS file (struct/typedef declarations merged in)
//!   - "utils/memutils.h"      -> crate::utils::mmgr::{mcxt,aset}
//!   - "utils/typcache.h"      -> crate::utils::cache::typcache
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/expandedrecord.c

use crate::prelude::*;
use crate::c::{int32, uint32, uint64, MAXALIGN};

use crate::access::common::detoast::detoast_external_attr;
use crate::access::common::heaptuple::{
    heap_compute_data_size, heap_copytuple, heap_deform_tuple, heap_fill_tuple, heap_form_tuple,
    heap_freetuple, heap_getsysattr,
};
use crate::access::common::tupdesc::{
    CompactAttribute, CreateTupleDescCopy, FreeTupleDesc, ReleaseTupleDesc, TupleDesc,
    TupleDescAttr, TupleDescCompactAttr,
};
use crate::access::heap::heaptoast::toast_flatten_tuple;
use crate::access::htup_details::{
    BITMAPLEN, HeapTupleData, HeapTupleHasExternal, HeapTupleHeader,
    HeapTupleHeaderGetDatumLength, HeapTupleHeaderGetTypMod, HeapTupleHeaderGetTypeId,
    HeapTupleHeaderHasExternal, HeapTupleHeaderSetDatumLength, HeapTupleHeaderSetNatts,
    HeapTupleHeaderSetTypMod, HeapTupleHeaderSetTypeId, SizeofHeapTupleHeader,
};
use crate::catalog::heap::SystemAttributeByName;
use crate::catalog::pg_attribute::{Form_pg_attribute, FormData_pg_attribute};
use crate::catalog::pg_type::TYPTYPE_DOMAIN;
use crate::catalog::pg_type_d::RECORDOID;
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::postgres_ext::InvalidOid;
use crate::storage::itemptr::ItemPointerSetInvalid;
use crate::utils::adt::datum::datumCopy;
use crate::utils::adt::domains::domain_check;
use crate::utils::adt::expandeddatum::{
    DatumGetEOHP, EOHPGetRODatum, EOHPGetRWDatum, EOH_init_header, ExpandedObjectHeader,
    ExpandedObjectMethods,
};
use crate::utils::adt::format_type::format_type_be;
use crate::utils::adt::name::namestrcmp;
use crate::utils::cache::typcache::{
    assign_record_type_identifier, assign_record_type_typmod, lookup_rowtype_tupdesc,
    lookup_type_cache, TypeCacheEntry, TYPECACHE_DOMAIN_BASE_INFO, TYPECACHE_TUPDESC,
};
use crate::utils::mmgr::aset::AllocSetContextCreate as AllocSetContextCreateFn;
use crate::utils::mmgr::mcxt::{
    MemoryContextAlloc, MemoryContextAllocZero, MemoryContextRegisterResetCallback,
    MemoryContextReset, MemoryContextSwitchTo,
};
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::palloc::{pfree, MemoryContextCallback};
use crate::varatt::{
    varlena, VARATT_IS_EXTERNAL, VARATT_IS_EXTERNAL_EXPANDED_RW,
};
use core::ffi::{c_char, c_int, c_void, CStr};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
//   Dependencies from OTHER .c files not yet translated, stubbed for now.
// ----------------------------------------------------------------------------

// fmgr.h: DatumGetHeapTupleHeader (with detoast). Not yet translated.
unsafe fn DatumGetHeapTupleHeader(_d: Datum) -> HeapTupleHeader {
    unimplemented!("DatumGetHeapTupleHeader (fmgr.h) not yet translated")
}

// access/detoast.h: VARATT_IS_EXTERNAL_EXPANDED on raw pointer for DatumGetExpandedRecord.
// detoast.h: VARATT_IS_EXTERNAL_EXPANDED_RW already imported from varatt.

// ----------------------------------------------------------------------------
//   expandedrecord.h declarations (merged in)
// ----------------------------------------------------------------------------

/// ER_MAGIC: ID for debugging crosschecks.
pub const ER_MAGIC: c_int = 1384727874;

pub const ER_FLAG_FVALUE_VALID: c_int = 0x0001; // fvalue is up to date?
pub const ER_FLAG_FVALUE_ALLOCED: c_int = 0x0002; // fvalue is local storage?
pub const ER_FLAG_DVALUES_VALID: c_int = 0x0004; // dvalues/dnulls are up to date?
pub const ER_FLAG_DVALUES_ALLOCED: c_int = 0x0008; // any field values local storage?
pub const ER_FLAG_HAVE_EXTERNAL: c_int = 0x0010; // any field values are external?
pub const ER_FLAG_TUPDESC_ALLOCED: c_int = 0x0020; // tupdesc is local storage?
pub const ER_FLAG_IS_DOMAIN: c_int = 0x0040; // er_decltypeid is domain?
pub const ER_FLAG_IS_DUMMY: c_int = 0x0080; // this header is dummy (see below)

/// flag bits that are not to be cleared when replacing tuple data:
pub const ER_FLAGS_NON_DATA: c_int =
    ER_FLAG_TUPDESC_ALLOCED | ER_FLAG_IS_DOMAIN | ER_FLAG_IS_DUMMY;

/// An expanded record is contained within a private memory context and has this
/// control structure.  Field order MUST match the C struct exactly.
#[repr(C)]
pub struct ExpandedRecordHeader {
    /// Standard header for expanded objects
    pub hdr: ExpandedObjectHeader,

    /// Magic value identifying an expanded record (for debugging only)
    pub er_magic: c_int,

    /// Assorted flag bits
    pub flags: c_int,

    /// Declared type of the record variable (could be a domain type)
    pub er_decltypeid: Oid,

    /// type OID of the composite type
    pub er_typeid: Oid,
    /// typmod of the composite type
    pub er_typmod: int32,

    /// Tuple descriptor, if we have one, else NULL.
    pub er_tupdesc: TupleDesc,

    /// Unique-within-process identifier for the tupdesc.
    pub er_tupdesc_id: uint64,

    /// array of Datums
    pub dvalues: *mut Datum,
    /// array of is-null flags for Datums
    pub dnulls: *mut bool,
    /// length of above arrays
    pub nfields: c_int,

    /// current space requirement for the flat equivalent, if known, else 0.
    pub flat_size: Size,

    /// data len within flat_size
    pub data_len: Size,
    /// header offset
    pub hoff: c_int,
    /// null bitmap needed?
    pub hasnull: bool,

    /// points to the flat representation if we have one, else NULL.
    pub fvalue: HeapTuple,
    /// start of its data area
    pub fstartptr: *mut c_char,
    /// end+1 of its data area
    pub fendptr: *mut c_char,

    /// short-term memory context
    pub er_short_term_cxt: MemoryContext,

    /// dummy record header
    pub er_dummy_header: *mut ExpandedRecordHeader,
    /// cache space for domain_check()
    pub er_domaininfo: *mut c_void,

    /// Callback info (it's active if er_mcb.arg is not NULL)
    pub er_mcb: MemoryContextCallback,
}

/// information returned by expanded_record_lookup_field()
#[repr(C)]
pub struct ExpandedRecordFieldInfo {
    /// field's attr number in record
    pub fnumber: c_int,
    /// field's type/typmod info
    pub ftypeid: Oid,
    pub ftypmod: int32,
    /// field's collation if any
    pub fcollation: Oid,
}

/// fmgr functions and macros for expanded record objects
#[inline]
pub unsafe fn ExpandedRecordGetDatum(erh: *const ExpandedRecordHeader) -> Datum {
    EOHPGetRWDatum(&(*erh).hdr)
}

#[inline]
pub unsafe fn ExpandedRecordGetRODatum(erh: *const ExpandedRecordHeader) -> Datum {
    EOHPGetRODatum(&(*erh).hdr)
}

/// #define ExpandedRecordIsEmpty(erh)
#[inline]
pub unsafe fn ExpandedRecordIsEmpty(erh: *const ExpandedRecordHeader) -> bool {
    ((*erh).flags & (ER_FLAG_DVALUES_VALID | ER_FLAG_FVALUE_VALID)) == 0
}

/// #define ExpandedRecordIsDomain(erh)
#[inline]
pub unsafe fn ExpandedRecordIsDomain(erh: *const ExpandedRecordHeader) -> bool {
    ((*erh).flags & ER_FLAG_IS_DOMAIN) != 0
}

/// Get the tupdesc for the expanded record's actual type
#[inline]
pub unsafe fn expanded_record_get_tupdesc(erh: *mut ExpandedRecordHeader) -> TupleDesc {
    if likely(!(*erh).er_tupdesc.is_null()) {
        (*erh).er_tupdesc
    } else {
        expanded_record_fetch_tupdesc(erh)
    }
}

/// Get value of record field
#[inline]
pub unsafe fn expanded_record_get_field(
    erh: *mut ExpandedRecordHeader,
    fnumber: c_int,
    isnull: *mut bool,
) -> Datum {
    if ((*erh).flags & ER_FLAG_DVALUES_VALID) != 0
        && likely(fnumber > 0 && fnumber <= (*erh).nfields)
    {
        *isnull = *(*erh).dnulls.add((fnumber - 1) as usize);
        *(*erh).dvalues.add((fnumber - 1) as usize)
    } else {
        expanded_record_fetch_field(erh, fnumber, isnull)
    }
}

/// #define expanded_record_set_field(...) ... check_constraints = true
#[inline]
pub unsafe fn expanded_record_set_field(
    erh: *mut ExpandedRecordHeader,
    fnumber: c_int,
    newValue: Datum,
    isnull: bool,
    expand_external: bool,
) {
    expanded_record_set_field_internal(erh, fnumber, newValue, isnull, expand_external, true);
}

// errcodes.h classification (errcode() shim ignores the value).
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 0;

// ----------------------------------------------------------------------------
//   "Methods" required for an expanded object
// ----------------------------------------------------------------------------

unsafe extern "C" fn ER_get_flat_size_method(eohptr: *mut ExpandedObjectHeader) -> Size {
    ER_get_flat_size(eohptr)
}

unsafe extern "C" fn ER_flatten_into_method(
    eohptr: *mut ExpandedObjectHeader,
    result: *mut c_void,
    allocated_size: Size,
) {
    ER_flatten_into(eohptr, result, allocated_size);
}

static ER_methods: ExpandedObjectMethods = ExpandedObjectMethods {
    get_flat_size: ER_get_flat_size_method,
    flatten_into: ER_flatten_into_method,
};

// ----------------------------------------------------------------------------
//   expandedrecord.c body
// ----------------------------------------------------------------------------

/// Build an expanded record of the specified composite type
///
/// type_id can be RECORDOID, but only if a positive typmod is given.
///
/// The expanded record is initially "empty", having a state logically
/// equivalent to a NULL composite value (not ROW(NULL, NULL, ...)).
/// Note that this might not be a valid state for a domain type;
/// if the caller needs to check that, call
/// expanded_record_set_tuple(erh, NULL, false, false).
///
/// The expanded object will be a child of parentcontext.
pub unsafe fn make_expanded_record_from_typeid(
    type_id: Oid,
    typmod: int32,
    parentcontext: MemoryContext,
) -> *mut ExpandedRecordHeader {
    let erh: *mut ExpandedRecordHeader;
    let mut flags: c_int = 0;
    let tupdesc: TupleDesc;
    let tupdesc_id: uint64;
    let objcxt: MemoryContext;
    let chunk: *mut c_char;

    if type_id != RECORDOID {
        /*
         * Consult the typcache to see if it's a domain over composite, and in
         * any case to get the tupdesc and tupdesc identifier.
         */
        let mut typentry: *mut TypeCacheEntry;

        typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO);
        if (*typentry).typtype == TYPTYPE_DOMAIN {
            flags |= ER_FLAG_IS_DOMAIN;
            typentry = lookup_type_cache((*typentry).domainBaseType, TYPECACHE_TUPDESC);
        }
        if (*typentry).tupDesc.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "type {} is not composite",
                    CStr::from_ptr(format_type_be(type_id)).to_string_lossy()
                )
            );
            // C also: errcode(ERRCODE_WRONG_OBJECT_TYPE)
        }
        tupdesc = (*typentry).tupDesc;
        tupdesc_id = (*typentry).tupDesc_identifier;
    } else {
        /*
         * For RECORD types, get the tupdesc and identifier from typcache.
         */
        tupdesc = lookup_rowtype_tupdesc(type_id, typmod);
        tupdesc_id = assign_record_type_identifier(type_id, typmod);
    }

    /*
     * Allocate private context for expanded object.  We use a regular-size
     * context, not a small one, to improve the odds that we can fit a tupdesc
     * into it without needing an extra malloc block.
     */
    objcxt = AllocSetContextCreateFn(
        parentcontext,
        b"expanded record\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES,
    );

    /*
     * Since we already know the number of fields in the tupdesc, we can
     * allocate the dvalues/dnulls arrays along with the record header.
     */
    erh = MemoryContextAlloc(
        objcxt,
        MAXALIGN(core::mem::size_of::<ExpandedRecordHeader>())
            + (*tupdesc).natts as usize
                * (core::mem::size_of::<Datum>() + core::mem::size_of::<bool>()),
    ) as *mut ExpandedRecordHeader;

    /* Ensure all header fields are initialized to 0/null */
    memset(
        erh as *mut c_void,
        0,
        core::mem::size_of::<ExpandedRecordHeader>(),
    );

    EOH_init_header(&mut (*erh).hdr, &ER_methods, objcxt);
    (*erh).er_magic = ER_MAGIC;

    /* Set up dvalues/dnulls, with no valid contents as yet */
    chunk = (erh as *mut c_char).add(MAXALIGN(core::mem::size_of::<ExpandedRecordHeader>()));
    (*erh).dvalues = chunk as *mut Datum;
    (*erh).dnulls = chunk.add((*tupdesc).natts as usize * core::mem::size_of::<Datum>()) as *mut bool;
    (*erh).nfields = (*tupdesc).natts;

    /* Fill in composite-type identification info */
    (*erh).er_decltypeid = type_id;
    (*erh).er_typeid = (*tupdesc).tdtypeid;
    (*erh).er_typmod = (*tupdesc).tdtypmod;
    (*erh).er_tupdesc_id = tupdesc_id;

    (*erh).flags = flags;

    /*
     * If what we got from the typcache is a refcounted tupdesc, we need to
     * acquire our own refcount on it.
     */
    if (*tupdesc).tdrefcount >= 0 {
        /* Register callback to release the refcount */
        (*erh).er_mcb.func = Some(ER_mc_callback);
        (*erh).er_mcb.arg = erh as *mut c_void;
        MemoryContextRegisterResetCallback((*erh).hdr.eoh_context, &mut (*erh).er_mcb);

        /* And save the pointer */
        (*erh).er_tupdesc = tupdesc;
        (*tupdesc).tdrefcount += 1;

        /* If we called lookup_rowtype_tupdesc, release the pin it took */
        if type_id == RECORDOID {
            ReleaseTupleDesc(tupdesc);
        }
    } else {
        /*
         * If it's not refcounted, just assume it will outlive the expanded
         * object.
         */
        (*erh).er_tupdesc = tupdesc;
    }

    /*
     * We don't set ER_FLAG_DVALUES_VALID or ER_FLAG_FVALUE_VALID, so the
     * record remains logically empty.
     */

    erh
}

/// Build an expanded record of the rowtype defined by the tupdesc
///
/// The tupdesc is copied if necessary (i.e., if we can't just bump its
/// reference count instead).
///
/// The expanded record is initially "empty", having a state logically
/// equivalent to a NULL composite value (not ROW(NULL, NULL, ...)).
///
/// The expanded object will be a child of parentcontext.
pub unsafe fn make_expanded_record_from_tupdesc(
    mut tupdesc: TupleDesc,
    parentcontext: MemoryContext,
) -> *mut ExpandedRecordHeader {
    let erh: *mut ExpandedRecordHeader;
    let tupdesc_id: uint64;
    let objcxt: MemoryContext;
    let oldcxt: MemoryContext;
    let chunk: *mut c_char;

    if (*tupdesc).tdtypeid != RECORDOID {
        /*
         * If it's a named composite type (not RECORD), we prefer to reference
         * the typcache's copy of the tupdesc, which is guaranteed to be
         * refcounted (the given tupdesc might not be).  In any case, we need
         * to consult the typcache to get the correct tupdesc identifier.
         *
         * Note that tdtypeid couldn't be a domain type, so we need not
         * consider that case here.
         */
        let typentry: *mut TypeCacheEntry;

        typentry = lookup_type_cache((*tupdesc).tdtypeid, TYPECACHE_TUPDESC);
        if (*typentry).tupDesc.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "type {} is not composite",
                    CStr::from_ptr(format_type_be((*tupdesc).tdtypeid)).to_string_lossy()
                )
            );
            // C also: errcode(ERRCODE_WRONG_OBJECT_TYPE)
        }
        tupdesc = (*typentry).tupDesc;
        tupdesc_id = (*typentry).tupDesc_identifier;
    } else {
        /*
         * For RECORD types, get the appropriate unique identifier (possibly
         * freshly assigned).
         */
        tupdesc_id = assign_record_type_identifier((*tupdesc).tdtypeid, (*tupdesc).tdtypmod);
    }

    /*
     * Allocate private context for expanded object.
     */
    objcxt = AllocSetContextCreateFn(
        parentcontext,
        b"expanded record\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES,
    );

    /*
     * Since we already know the number of fields in the tupdesc, we can
     * allocate the dvalues/dnulls arrays along with the record header.
     */
    erh = MemoryContextAlloc(
        objcxt,
        MAXALIGN(core::mem::size_of::<ExpandedRecordHeader>())
            + (*tupdesc).natts as usize
                * (core::mem::size_of::<Datum>() + core::mem::size_of::<bool>()),
    ) as *mut ExpandedRecordHeader;

    /* Ensure all header fields are initialized to 0/null */
    memset(
        erh as *mut c_void,
        0,
        core::mem::size_of::<ExpandedRecordHeader>(),
    );

    EOH_init_header(&mut (*erh).hdr, &ER_methods, objcxt);
    (*erh).er_magic = ER_MAGIC;

    /* Set up dvalues/dnulls, with no valid contents as yet */
    chunk = (erh as *mut c_char).add(MAXALIGN(core::mem::size_of::<ExpandedRecordHeader>()));
    (*erh).dvalues = chunk as *mut Datum;
    (*erh).dnulls = chunk.add((*tupdesc).natts as usize * core::mem::size_of::<Datum>()) as *mut bool;
    (*erh).nfields = (*tupdesc).natts;

    /* Fill in composite-type identification info */
    (*erh).er_typeid = (*tupdesc).tdtypeid;
    (*erh).er_decltypeid = (*erh).er_typeid;
    (*erh).er_typmod = (*tupdesc).tdtypmod;
    (*erh).er_tupdesc_id = tupdesc_id;

    /*
     * Copy tupdesc if needed, but we prefer to bump its refcount if possible.
     */
    if (*tupdesc).tdrefcount >= 0 {
        /* Register callback to release the refcount */
        (*erh).er_mcb.func = Some(ER_mc_callback);
        (*erh).er_mcb.arg = erh as *mut c_void;
        MemoryContextRegisterResetCallback((*erh).hdr.eoh_context, &mut (*erh).er_mcb);

        /* And save the pointer */
        (*erh).er_tupdesc = tupdesc;
        (*tupdesc).tdrefcount += 1;
    } else {
        /* Just copy it */
        oldcxt = MemoryContextSwitchTo(objcxt);
        (*erh).er_tupdesc = CreateTupleDescCopy(tupdesc);
        (*erh).flags |= ER_FLAG_TUPDESC_ALLOCED;
        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * We don't set ER_FLAG_DVALUES_VALID or ER_FLAG_FVALUE_VALID, so the
     * record remains logically empty.
     */

    erh
}

/// Build an expanded record of the same rowtype as the given expanded record
///
/// This is faster than either of the above routines because we can bypass
/// typcache lookup(s).
///
/// The expanded record is initially "empty" --- we do not copy whatever
/// tuple might be in the source expanded record.
///
/// The expanded object will be a child of parentcontext.
pub unsafe fn make_expanded_record_from_exprecord(
    olderh: *mut ExpandedRecordHeader,
    parentcontext: MemoryContext,
) -> *mut ExpandedRecordHeader {
    let erh: *mut ExpandedRecordHeader;
    let tupdesc: TupleDesc = expanded_record_get_tupdesc(olderh);
    let objcxt: MemoryContext;
    let oldcxt: MemoryContext;
    let chunk: *mut c_char;

    /*
     * Allocate private context for expanded object.
     */
    objcxt = AllocSetContextCreateFn(
        parentcontext,
        b"expanded record\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES,
    );

    /*
     * Since we already know the number of fields in the tupdesc, we can
     * allocate the dvalues/dnulls arrays along with the record header.
     */
    erh = MemoryContextAlloc(
        objcxt,
        MAXALIGN(core::mem::size_of::<ExpandedRecordHeader>())
            + (*tupdesc).natts as usize
                * (core::mem::size_of::<Datum>() + core::mem::size_of::<bool>()),
    ) as *mut ExpandedRecordHeader;

    /* Ensure all header fields are initialized to 0/null */
    memset(
        erh as *mut c_void,
        0,
        core::mem::size_of::<ExpandedRecordHeader>(),
    );

    EOH_init_header(&mut (*erh).hdr, &ER_methods, objcxt);
    (*erh).er_magic = ER_MAGIC;

    /* Set up dvalues/dnulls, with no valid contents as yet */
    chunk = (erh as *mut c_char).add(MAXALIGN(core::mem::size_of::<ExpandedRecordHeader>()));
    (*erh).dvalues = chunk as *mut Datum;
    (*erh).dnulls = chunk.add((*tupdesc).natts as usize * core::mem::size_of::<Datum>()) as *mut bool;
    (*erh).nfields = (*tupdesc).natts;

    /* Fill in composite-type identification info */
    (*erh).er_decltypeid = (*olderh).er_decltypeid;
    (*erh).er_typeid = (*olderh).er_typeid;
    (*erh).er_typmod = (*olderh).er_typmod;
    (*erh).er_tupdesc_id = (*olderh).er_tupdesc_id;

    /* The only flag bit that transfers over is IS_DOMAIN */
    (*erh).flags = (*olderh).flags & ER_FLAG_IS_DOMAIN;

    /*
     * Copy tupdesc if needed, but we prefer to bump its refcount if possible.
     */
    if (*tupdesc).tdrefcount >= 0 {
        /* Register callback to release the refcount */
        (*erh).er_mcb.func = Some(ER_mc_callback);
        (*erh).er_mcb.arg = erh as *mut c_void;
        MemoryContextRegisterResetCallback((*erh).hdr.eoh_context, &mut (*erh).er_mcb);

        /* And save the pointer */
        (*erh).er_tupdesc = tupdesc;
        (*tupdesc).tdrefcount += 1;
    } else if ((*olderh).flags & ER_FLAG_TUPDESC_ALLOCED) != 0 {
        /* We need to make our own copy of the tupdesc */
        oldcxt = MemoryContextSwitchTo(objcxt);
        (*erh).er_tupdesc = CreateTupleDescCopy(tupdesc);
        (*erh).flags |= ER_FLAG_TUPDESC_ALLOCED;
        MemoryContextSwitchTo(oldcxt);
    } else {
        /*
         * Assume the tupdesc will outlive this expanded object, just like
         * we're assuming it will outlive the source object.
         */
        (*erh).er_tupdesc = tupdesc;
    }

    /*
     * We don't set ER_FLAG_DVALUES_VALID or ER_FLAG_FVALUE_VALID, so the
     * record remains logically empty.
     */

    erh
}

/// Insert given tuple as the value of the expanded record
///
/// It is caller's responsibility that the tuple matches the record's
/// previously-assigned rowtype.  (However domain constraints, if any,
/// will be checked here.)
///
/// The tuple is physically copied into the expanded record's local storage
/// if "copy" is true, otherwise it's caller's responsibility that the tuple
/// will live as long as the expanded record does.
///
/// Out-of-line field values in the tuple are automatically inlined if
/// "expand_external" is true, otherwise not.  (The combination copy = false,
/// expand_external = true is not sensible and not supported.)
///
/// Alternatively, tuple can be NULL, in which case we just set the expanded
/// record to be empty.
pub unsafe fn expanded_record_set_tuple(
    erh: *mut ExpandedRecordHeader,
    mut tuple: HeapTuple,
    copy: bool,
    mut expand_external: bool,
) {
    let oldflags: c_int;
    let oldtuple: HeapTuple;
    let oldfstartptr: *mut c_char;
    let oldfendptr: *mut c_char;
    let mut newflags: c_int;
    let newtuple: HeapTuple;
    let mut oldcxt: MemoryContext;

    /* Shouldn't ever be trying to assign new data to a dummy header */
    Assert!(((*erh).flags & ER_FLAG_IS_DUMMY) == 0);

    /*
     * Before performing the assignment, see if result will satisfy domain.
     */
    if ((*erh).flags & ER_FLAG_IS_DOMAIN) != 0 {
        check_domain_for_new_tuple(erh, tuple);
    }

    /*
     * If we need to get rid of out-of-line field values, do so, using the
     * short-term context to avoid leaking whatever cruft the toast fetch
     * might generate.
     */
    if expand_external && !tuple.is_null() {
        /* Assert caller didn't ask for unsupported case */
        Assert!(copy);
        if HeapTupleHasExternal(tuple) {
            oldcxt = MemoryContextSwitchTo(get_short_term_cxt(erh));
            tuple = toast_flatten_tuple(tuple, (*erh).er_tupdesc);
            MemoryContextSwitchTo(oldcxt);
        } else {
            expand_external = false; /* need not clean up below */
        }
    }

    /*
     * Initialize new flags, keeping only non-data status bits.
     */
    oldflags = (*erh).flags;
    newflags = oldflags & ER_FLAGS_NON_DATA;

    /*
     * Copy tuple into local storage if needed.
     */
    if copy && !tuple.is_null() {
        oldcxt = MemoryContextSwitchTo((*erh).hdr.eoh_context);
        newtuple = heap_copytuple(tuple);
        newflags |= ER_FLAG_FVALUE_ALLOCED;
        MemoryContextSwitchTo(oldcxt);

        /* We can now flush anything that detoasting might have leaked. */
        if expand_external {
            MemoryContextReset((*erh).er_short_term_cxt);
        }
    } else {
        newtuple = tuple;
    }

    /* Make copies of fields we're about to overwrite */
    oldtuple = (*erh).fvalue;
    oldfstartptr = (*erh).fstartptr;
    oldfendptr = (*erh).fendptr;

    /*
     * It's now safe to update the expanded record's state.
     */
    if !newtuple.is_null() {
        /* Save flat representation */
        (*erh).fvalue = newtuple;
        (*erh).fstartptr = (*newtuple).t_data as *mut c_char;
        (*erh).fendptr = ((*newtuple).t_data as *mut c_char).add((*newtuple).t_len as usize);
        newflags |= ER_FLAG_FVALUE_VALID;

        /* Remember if we have any out-of-line field values */
        if HeapTupleHasExternal(newtuple) {
            newflags |= ER_FLAG_HAVE_EXTERNAL;
        }
    } else {
        (*erh).fvalue = null_mut();
        (*erh).fendptr = null_mut();
        (*erh).fstartptr = (*erh).fendptr;
    }

    (*erh).flags = newflags;

    /* Reset flat-size info; we don't bother to make it valid now */
    (*erh).flat_size = 0;

    /*
     * Now, release any storage belonging to old field values.
     */
    if (oldflags & ER_FLAG_DVALUES_ALLOCED) != 0 {
        let tupdesc: TupleDesc = (*erh).er_tupdesc;
        let mut i: c_int;

        i = 0;
        while i < (*erh).nfields {
            if !*(*erh).dnulls.add(i as usize)
                && !(*TupleDescAttr(tupdesc, i)).attbyval
            {
                let oldValue: *mut c_char =
                    DatumGetPointer(*(*erh).dvalues.add(i as usize)) as *mut c_char;

                if oldValue < oldfstartptr || oldValue >= oldfendptr {
                    pfree(oldValue as *mut c_void);
                }
            }
            i += 1;
        }
    }

    /* Likewise free the old tuple, if it was locally allocated */
    if (oldflags & ER_FLAG_FVALUE_ALLOCED) != 0 {
        heap_freetuple(oldtuple);
    }

    /* We won't make a new deconstructed representation until/unless needed */
}

/// make_expanded_record_from_datum: build expanded record from composite Datum
///
/// This combines the functions of make_expanded_record_from_typeid and
/// expanded_record_set_tuple.  However, we do not force a lookup of the
/// tupdesc immediately, reasoning that it might never be needed.
///
/// The expanded object will be a child of parentcontext.
///
/// Note: a composite datum cannot self-identify as being of a domain type,
/// so we need not consider domain cases here.
pub unsafe fn make_expanded_record_from_datum(
    recorddatum: Datum,
    parentcontext: MemoryContext,
) -> Datum {
    let erh: *mut ExpandedRecordHeader;
    let tuphdr: HeapTupleHeader;
    let mut tmptup: HeapTupleData = core::mem::zeroed();
    let newtuple: HeapTuple;
    let objcxt: MemoryContext;
    let oldcxt: MemoryContext;

    /*
     * Allocate private context for expanded object.
     */
    objcxt = AllocSetContextCreateFn(
        parentcontext,
        b"expanded record\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES,
    );

    /* Set up expanded record header, initializing fields to 0/null */
    erh = MemoryContextAllocZero(objcxt, core::mem::size_of::<ExpandedRecordHeader>())
        as *mut ExpandedRecordHeader;

    EOH_init_header(&mut (*erh).hdr, &ER_methods, objcxt);
    (*erh).er_magic = ER_MAGIC;

    /*
     * Detoast and copy source record into private context, as a HeapTuple.
     */
    tuphdr = DatumGetHeapTupleHeader(recorddatum);

    tmptup.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
    ItemPointerSetInvalid(&mut tmptup.t_self);
    tmptup.t_tableOid = InvalidOid;
    tmptup.t_data = tuphdr;

    oldcxt = MemoryContextSwitchTo(objcxt);
    newtuple = heap_copytuple(&mut tmptup);
    (*erh).flags |= ER_FLAG_FVALUE_ALLOCED;
    MemoryContextSwitchTo(oldcxt);

    /* Fill in composite-type identification info */
    (*erh).er_typeid = HeapTupleHeaderGetTypeId(tuphdr);
    (*erh).er_decltypeid = (*erh).er_typeid;
    (*erh).er_typmod = HeapTupleHeaderGetTypMod(tuphdr);

    /* remember we have a flat representation */
    (*erh).fvalue = newtuple;
    (*erh).fstartptr = (*newtuple).t_data as *mut c_char;
    (*erh).fendptr = ((*newtuple).t_data as *mut c_char).add((*newtuple).t_len as usize);
    (*erh).flags |= ER_FLAG_FVALUE_VALID;

    /* Shouldn't need to set ER_FLAG_HAVE_EXTERNAL */
    Assert!(!HeapTupleHeaderHasExternal(tuphdr));

    /*
     * We won't look up the tupdesc till we have to, nor make a deconstructed
     * representation.  We don't have enough info to fill flat_size and
     * friends, either.
     */

    /* return a R/W pointer to the expanded record */
    EOHPGetRWDatum(&(*erh).hdr)
}

/// get_flat_size method for expanded records
///
/// Note: call this in a reasonably short-lived memory context, in case of
/// memory leaks from activities such as detoasting.
unsafe fn ER_get_flat_size(eohptr: *mut ExpandedObjectHeader) -> Size {
    let erh: *mut ExpandedRecordHeader = eohptr as *mut ExpandedRecordHeader;
    let tupdesc: TupleDesc;
    let mut len: Size;
    let data_len: Size;
    let hoff: c_int;
    let mut hasnull: bool;
    let mut i: c_int;

    Assert!((*erh).er_magic == ER_MAGIC);

    /*
     * The flat representation has to be a valid composite datum.  Make sure
     * that we have a registered, not anonymous, RECORD type.
     */
    if (*erh).er_typeid == RECORDOID && (*erh).er_typmod < 0 {
        let tupdesc = expanded_record_get_tupdesc(erh);
        assign_record_type_typmod(tupdesc);
        (*erh).er_typmod = (*tupdesc).tdtypmod;
    }

    /*
     * If we have a valid flattened value without out-of-line fields, we can
     * just use it as-is.
     */
    if ((*erh).flags & ER_FLAG_FVALUE_VALID) != 0
        && ((*erh).flags & ER_FLAG_HAVE_EXTERNAL) == 0
    {
        return (*(*erh).fvalue).t_len as Size;
    }

    /* If we have a cached size value, believe that */
    if (*erh).flat_size != 0 {
        return (*erh).flat_size;
    }

    /* If we haven't yet deconstructed the tuple, do that */
    if ((*erh).flags & ER_FLAG_DVALUES_VALID) == 0 {
        deconstruct_expanded_record(erh);
    }

    /* Tuple descriptor must be valid by now */
    tupdesc = (*erh).er_tupdesc;

    /*
     * Composite datums mustn't contain any out-of-line values.
     */
    if ((*erh).flags & ER_FLAG_HAVE_EXTERNAL) != 0 {
        i = 0;
        while i < (*erh).nfields {
            let attr: *mut CompactAttribute = TupleDescCompactAttr(tupdesc, i);

            if !*(*erh).dnulls.add(i as usize)
                && !(*attr).attbyval
                && (*attr).attlen == -1
                && VARATT_IS_EXTERNAL(DatumGetPointer(*(*erh).dvalues.add(i as usize)))
            {
                /*
                 * expanded_record_set_field_internal can do the actual work
                 * of detoasting.  It needn't recheck domain constraints.
                 */
                expanded_record_set_field_internal(
                    erh,
                    i + 1,
                    *(*erh).dvalues.add(i as usize),
                    false,
                    true,
                    false,
                );
            }
            i += 1;
        }

        /*
         * We have now removed all external field values, so we can clear the
         * flag about them.
         */
        (*erh).flags &= !ER_FLAG_HAVE_EXTERNAL;
    }

    /* Test if we currently have any null values */
    hasnull = false;
    i = 0;
    while i < (*erh).nfields {
        if *(*erh).dnulls.add(i as usize) {
            hasnull = true;
            break;
        }
        i += 1;
    }

    /* Determine total space needed */
    len = SizeofHeapTupleHeader;

    if hasnull {
        len += BITMAPLEN((*tupdesc).natts) as Size;
    }

    len = MAXALIGN(len); /* align user data safely */
    hoff = len as c_int;

    data_len = heap_compute_data_size(tupdesc, (*erh).dvalues, (*erh).dnulls);

    len += data_len;

    /* Cache for next time */
    (*erh).flat_size = len;
    (*erh).data_len = data_len;
    (*erh).hoff = hoff;
    (*erh).hasnull = hasnull;

    len
}

/// flatten_into method for expanded records
unsafe fn ER_flatten_into(
    eohptr: *mut ExpandedObjectHeader,
    result: *mut c_void,
    allocated_size: Size,
) {
    let erh: *mut ExpandedRecordHeader = eohptr as *mut ExpandedRecordHeader;
    let tuphdr: HeapTupleHeader = result as HeapTupleHeader;
    let tupdesc: TupleDesc;

    Assert!((*erh).er_magic == ER_MAGIC);

    /* Easy if we have a valid flattened value without out-of-line fields */
    if ((*erh).flags & ER_FLAG_FVALUE_VALID) != 0
        && ((*erh).flags & ER_FLAG_HAVE_EXTERNAL) == 0
    {
        Assert!(allocated_size == (*(*erh).fvalue).t_len as Size);
        memcpy(
            tuphdr as *mut c_void,
            (*(*erh).fvalue).t_data as *const c_void,
            allocated_size,
        );
        /* The original flattened value might not have datum header fields */
        HeapTupleHeaderSetDatumLength(tuphdr, allocated_size as uint32);
        HeapTupleHeaderSetTypeId(tuphdr, (*erh).er_typeid);
        HeapTupleHeaderSetTypMod(tuphdr, (*erh).er_typmod);
        return;
    }

    /* Else allocation should match previous get_flat_size result */
    Assert!(allocated_size == (*erh).flat_size);

    /* We'll need the tuple descriptor */
    tupdesc = expanded_record_get_tupdesc(erh);

    /* We must ensure that any pad space is zero-filled */
    memset(tuphdr as *mut c_void, 0, allocated_size);

    /* Set up header fields of composite Datum */
    HeapTupleHeaderSetDatumLength(tuphdr, allocated_size as uint32);
    HeapTupleHeaderSetTypeId(tuphdr, (*erh).er_typeid);
    HeapTupleHeaderSetTypMod(tuphdr, (*erh).er_typmod);
    /* We also make sure that t_ctid is invalid unless explicitly set */
    ItemPointerSetInvalid(&mut (*tuphdr).t_ctid);

    HeapTupleHeaderSetNatts(tuphdr, (*tupdesc).natts as u16);
    (*tuphdr).t_hoff = (*erh).hoff as u8;

    /* And fill the data area from dvalues/dnulls */
    heap_fill_tuple(
        tupdesc,
        (*erh).dvalues,
        (*erh).dnulls,
        (tuphdr as *mut c_char).add((*erh).hoff as usize),
        (*erh).data_len,
        &mut (*tuphdr).t_infomask,
        if (*erh).hasnull {
            (*tuphdr).t_bits.as_mut_ptr()
        } else {
            null_mut()
        },
    );
}

/// Look up the tupdesc for the expanded record's actual type
///
/// Note: code internal to this module is allowed to just fetch
/// erh->er_tupdesc if ER_FLAG_DVALUES_VALID is set; otherwise it should call
/// expanded_record_get_tupdesc.  This function is the out-of-line portion
/// of expanded_record_get_tupdesc.
pub unsafe fn expanded_record_fetch_tupdesc(erh: *mut ExpandedRecordHeader) -> TupleDesc {
    let tupdesc: TupleDesc;

    /* Easy if we already have it (but caller should have checked already) */
    if !(*erh).er_tupdesc.is_null() {
        return (*erh).er_tupdesc;
    }

    /* Lookup the composite type's tupdesc using the typcache */
    tupdesc = lookup_rowtype_tupdesc((*erh).er_typeid, (*erh).er_typmod);

    /*
     * If it's a refcounted tupdesc rather than a statically allocated one, we
     * want to manage the refcount with a memory context callback.
     */
    if (*tupdesc).tdrefcount >= 0 {
        /* Register callback if we didn't already */
        if (*erh).er_mcb.arg.is_null() {
            (*erh).er_mcb.func = Some(ER_mc_callback);
            (*erh).er_mcb.arg = erh as *mut c_void;
            MemoryContextRegisterResetCallback((*erh).hdr.eoh_context, &mut (*erh).er_mcb);
        }

        /* Remember our own pointer */
        (*erh).er_tupdesc = tupdesc;
        (*tupdesc).tdrefcount += 1;

        /* Release the pin lookup_rowtype_tupdesc acquired */
        ReleaseTupleDesc(tupdesc);
    } else {
        /* Just remember the pointer */
        (*erh).er_tupdesc = tupdesc;
    }

    /* In either case, fetch the process-global ID for this tupdesc */
    (*erh).er_tupdesc_id =
        assign_record_type_identifier((*tupdesc).tdtypeid, (*tupdesc).tdtypmod);

    tupdesc
}

/// Get a HeapTuple representing the current value of the expanded record
///
/// If valid, the originally stored tuple is returned, so caller must not
/// scribble on it.  Otherwise, we return a HeapTuple created in the current
/// memory context.  In either case, no attempt has been made to inline
/// out-of-line toasted values, so the tuple isn't usable as a composite
/// datum.
///
/// Returns NULL if expanded record is empty.
pub unsafe fn expanded_record_get_tuple(erh: *mut ExpandedRecordHeader) -> HeapTuple {
    /* Easy case if we still have original tuple */
    if ((*erh).flags & ER_FLAG_FVALUE_VALID) != 0 {
        return (*erh).fvalue;
    }

    /* Else just build a tuple from datums */
    if ((*erh).flags & ER_FLAG_DVALUES_VALID) != 0 {
        return heap_form_tuple((*erh).er_tupdesc, (*erh).dvalues, (*erh).dnulls);
    }

    /* Expanded record is empty */
    null_mut()
}

/// Memory context reset callback for cleaning up external resources
unsafe extern "C" fn ER_mc_callback(arg: *mut c_void) {
    let erh: *mut ExpandedRecordHeader = arg as *mut ExpandedRecordHeader;
    let tupdesc: TupleDesc = (*erh).er_tupdesc;

    /* Release our privately-managed tupdesc refcount, if any */
    if !tupdesc.is_null() {
        (*erh).er_tupdesc = null_mut(); /* just for luck */
        if (*tupdesc).tdrefcount > 0 {
            (*tupdesc).tdrefcount -= 1;
            if (*tupdesc).tdrefcount == 0 {
                FreeTupleDesc(tupdesc);
            }
        }
    }
}

/// DatumGetExpandedRecord: get a writable expanded record from an input argument
///
/// Caution: if the input is a read/write pointer, this returns the input
/// argument; so callers must be sure that their changes are "safe", that is
/// they cannot leave the record in a corrupt state.
pub unsafe fn DatumGetExpandedRecord(mut d: Datum) -> *mut ExpandedRecordHeader {
    /* If it's a writable expanded record already, just return it */
    if VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d) as *const c_char) {
        let erh: *mut ExpandedRecordHeader = DatumGetEOHP(d) as *mut ExpandedRecordHeader;

        Assert!((*erh).er_magic == ER_MAGIC);
        return erh;
    }

    /* Else expand the hard way */
    d = make_expanded_record_from_datum(d, CurrentMemoryContext);
    DatumGetEOHP(d) as *mut ExpandedRecordHeader
}

/// Create the Datum/isnull representation of an expanded record object
/// if we didn't do so already.  After calling this, it's OK to read the
/// dvalues/dnulls arrays directly, rather than going through get_field.
///
/// Note that if the object is currently empty ("null"), this will change
/// it to represent a row of nulls.
pub unsafe fn deconstruct_expanded_record(erh: *mut ExpandedRecordHeader) {
    let tupdesc: TupleDesc;
    let dvalues: *mut Datum;
    let dnulls: *mut bool;
    let nfields: c_int;

    if ((*erh).flags & ER_FLAG_DVALUES_VALID) != 0 {
        return; /* already valid, nothing to do */
    }

    /* We'll need the tuple descriptor */
    tupdesc = expanded_record_get_tupdesc(erh);

    /*
     * Allocate arrays in private context, if we don't have them already.
     */
    nfields = (*tupdesc).natts;
    if (*erh).dvalues.is_null() || (*erh).nfields != nfields {
        let chunk: *mut c_char;

        /*
         * To save a palloc cycle, we allocate both the Datum and isnull
         * arrays in one palloc chunk.
         */
        chunk = MemoryContextAlloc(
            (*erh).hdr.eoh_context,
            nfields as usize * (core::mem::size_of::<Datum>() + core::mem::size_of::<bool>()),
        ) as *mut c_char;
        dvalues = chunk as *mut Datum;
        dnulls = chunk.add(nfields as usize * core::mem::size_of::<Datum>()) as *mut bool;
        (*erh).dvalues = dvalues;
        (*erh).dnulls = dnulls;
        (*erh).nfields = nfields;
    } else {
        dvalues = (*erh).dvalues;
        dnulls = (*erh).dnulls;
    }

    if ((*erh).flags & ER_FLAG_FVALUE_VALID) != 0 {
        /* Deconstruct tuple */
        heap_deform_tuple((*erh).fvalue, tupdesc, dvalues, dnulls);
    } else {
        /* If record was empty, instantiate it as a row of nulls */
        memset(dvalues as *mut c_void, 0, nfields as usize * core::mem::size_of::<Datum>());
        memset(dnulls as *mut c_void, true as c_int, nfields as usize * core::mem::size_of::<bool>());
    }

    /* Mark the dvalues as valid */
    (*erh).flags |= ER_FLAG_DVALUES_VALID;
}

/// Look up a record field by name
///
/// If there is a field named "fieldname", fill in the contents of finfo
/// and return "true".  Else return "false" without changing *finfo.
pub unsafe fn expanded_record_lookup_field(
    erh: *mut ExpandedRecordHeader,
    fieldname: *const c_char,
    finfo: *mut ExpandedRecordFieldInfo,
) -> bool {
    let tupdesc: TupleDesc;
    let mut fno: c_int;
    let attr: Form_pg_attribute;
    let sysattr: *const FormData_pg_attribute;

    tupdesc = expanded_record_get_tupdesc(erh);

    /* First, check user-defined attributes */
    fno = 0;
    while fno < (*tupdesc).natts {
        let attr: Form_pg_attribute = TupleDescAttr(tupdesc, fno);
        if namestrcmp(&mut (*attr).attname, fieldname) == 0 && !(*attr).attisdropped {
            (*finfo).fnumber = (*attr).attnum as c_int;
            (*finfo).ftypeid = (*attr).atttypid;
            (*finfo).ftypmod = (*attr).atttypmod;
            (*finfo).fcollation = (*attr).attcollation;
            return true;
        }
        fno += 1;
    }
    let _ = attr;

    /* How about system attributes? */
    sysattr = SystemAttributeByName(fieldname);
    if !sysattr.is_null() {
        (*finfo).fnumber = (*sysattr).attnum as c_int;
        (*finfo).ftypeid = (*sysattr).atttypid;
        (*finfo).ftypmod = (*sysattr).atttypmod;
        (*finfo).fcollation = (*sysattr).attcollation;
        return true;
    }

    false
}

/// Fetch value of record field
///
/// expanded_record_get_field is the frontend for this; it handles the
/// easy inline-able cases.
pub unsafe fn expanded_record_fetch_field(
    erh: *mut ExpandedRecordHeader,
    fnumber: c_int,
    isnull: *mut bool,
) -> Datum {
    if fnumber > 0 {
        /* Empty record has null fields */
        if ExpandedRecordIsEmpty(erh) {
            *isnull = true;
            return 0 as Datum;
        }
        /* Make sure we have deconstructed form */
        deconstruct_expanded_record(erh);
        /* Out-of-range field number reads as null */
        if unlikely(fnumber > (*erh).nfields) {
            *isnull = true;
            return 0 as Datum;
        }
        *isnull = *(*erh).dnulls.add((fnumber - 1) as usize);
        *(*erh).dvalues.add((fnumber - 1) as usize)
    } else {
        /* System columns read as null if we haven't got flat tuple */
        if (*erh).fvalue.is_null() {
            *isnull = true;
            return 0 as Datum;
        }
        /* heap_getsysattr doesn't actually use tupdesc, so just pass null */
        heap_getsysattr((*erh).fvalue, fnumber, null_mut(), isnull)
    }
}

/// Set value of record field
///
/// If the expanded record is of domain type, the assignment will be rejected
/// (without changing the record's state) if the domain's constraints would
/// be violated.
///
/// If expand_external is true and newValue is an out-of-line value, we'll
/// forcibly detoast it so that the record does not depend on external storage.
///
/// Internal callers can pass check_constraints = false to skip application
/// of domain constraints.  External callers should never do that.
pub unsafe fn expanded_record_set_field_internal(
    erh: *mut ExpandedRecordHeader,
    fnumber: c_int,
    mut newValue: Datum,
    isnull: bool,
    mut expand_external: bool,
    check_constraints: bool,
) {
    let tupdesc: TupleDesc;
    let attr: *mut CompactAttribute;
    let dvalues: *mut Datum;
    let dnulls: *mut bool;
    let oldValue: *mut c_char;

    /*
     * Shouldn't ever be trying to assign new data to a dummy header, except
     * in the case of an internal call for field inlining.
     */
    Assert!(((*erh).flags & ER_FLAG_IS_DUMMY) == 0 || !check_constraints);

    /* Before performing the assignment, see if result will satisfy domain */
    if ((*erh).flags & ER_FLAG_IS_DOMAIN) != 0 && check_constraints {
        check_domain_for_new_field(erh, fnumber, newValue, isnull);
    }

    /* If we haven't yet deconstructed the tuple, do that */
    if ((*erh).flags & ER_FLAG_DVALUES_VALID) == 0 {
        deconstruct_expanded_record(erh);
    }

    /* Tuple descriptor must be valid by now */
    tupdesc = (*erh).er_tupdesc;
    Assert!((*erh).nfields == (*tupdesc).natts);

    /* Caller error if fnumber is system column or nonexistent column */
    if unlikely(fnumber <= 0 || fnumber > (*erh).nfields) {
        elog!(ERROR, "cannot assign to field {} of expanded record", fnumber);
    }

    /*
     * Copy new field value into record's context, and deal with detoasting,
     * if needed.
     */
    attr = TupleDescCompactAttr(tupdesc, fnumber - 1);
    if !isnull && !(*attr).attbyval {
        let mut oldcxt: MemoryContext;

        /* If requested, detoast any external value */
        if expand_external {
            if (*attr).attlen == -1
                && VARATT_IS_EXTERNAL(DatumGetPointer(newValue))
            {
                /* Detoasting should be done in short-lived context. */
                oldcxt = MemoryContextSwitchTo(get_short_term_cxt(erh));
                newValue = PointerGetDatum(detoast_external_attr(
                    DatumGetPointer(newValue) as *mut varlena,
                ) as *const c_void);
                MemoryContextSwitchTo(oldcxt);
            } else {
                expand_external = false; /* need not clean up below */
            }
        }

        /* Copy value into record's context */
        oldcxt = MemoryContextSwitchTo((*erh).hdr.eoh_context);
        newValue = datumCopy(newValue, false, (*attr).attlen as c_int);
        MemoryContextSwitchTo(oldcxt);

        /* We can now flush anything that detoasting might have leaked */
        if expand_external {
            MemoryContextReset((*erh).er_short_term_cxt);
        }

        /* Remember that we have field(s) that may need to be pfree'd */
        (*erh).flags |= ER_FLAG_DVALUES_ALLOCED;

        /*
         * While we're here, note whether it's an external toasted value,
         * because that could mean we need to inline it later.
         */
        if (*attr).attlen == -1
            && VARATT_IS_EXTERNAL(DatumGetPointer(newValue))
        {
            (*erh).flags |= ER_FLAG_HAVE_EXTERNAL;
        }
    }

    /*
     * We're ready to make irreversible changes.
     */
    dvalues = (*erh).dvalues;
    dnulls = (*erh).dnulls;

    /* Flattened value will no longer represent record accurately */
    (*erh).flags &= !ER_FLAG_FVALUE_VALID;
    /* And we don't know the flattened size either */
    (*erh).flat_size = 0;

    /* Grab old field value for pfree'ing, if needed. */
    if !(*attr).attbyval && !*dnulls.add((fnumber - 1) as usize) {
        oldValue = DatumGetPointer(*dvalues.add((fnumber - 1) as usize)) as *mut c_char;
    } else {
        oldValue = null_mut();
    }

    /* And finally we can insert the new field. */
    *dvalues.add((fnumber - 1) as usize) = newValue;
    *dnulls.add((fnumber - 1) as usize) = isnull;

    /*
     * Free old field if needed; this keeps repeated field replacements from
     * bloating the record's storage.
     *
     * If we're updating a dummy header, we can't risk pfree'ing the old
     * value, because most likely the expanded record's main header still has
     * a pointer to it.
     */
    if !oldValue.is_null() && ((*erh).flags & ER_FLAG_IS_DUMMY) == 0 {
        /* Don't try to pfree a part of the original flat record */
        if oldValue < (*erh).fstartptr || oldValue >= (*erh).fendptr {
            pfree(oldValue as *mut c_void);
        }
    }
}

/// Set all record field(s)
///
/// Caller must ensure that the provided datums are of the right types
/// to match the record's previously assigned rowtype.
///
/// If expand_external is true, we'll forcibly detoast out-of-line field values
/// so that the record does not depend on external storage.
///
/// Unlike repeated application of expanded_record_set_field(), this does not
/// guarantee to leave the expanded record in a non-corrupt state in event
/// of an error.
pub unsafe fn expanded_record_set_fields(
    erh: *mut ExpandedRecordHeader,
    newValues: *const Datum,
    isnulls: *const bool,
    expand_external: bool,
) {
    let tupdesc: TupleDesc;
    let dvalues: *mut Datum;
    let dnulls: *mut bool;
    let mut fnumber: c_int;
    let oldcxt: MemoryContext;

    /* Shouldn't ever be trying to assign new data to a dummy header */
    Assert!(((*erh).flags & ER_FLAG_IS_DUMMY) == 0);

    /* If we haven't yet deconstructed the tuple, do that */
    if ((*erh).flags & ER_FLAG_DVALUES_VALID) == 0 {
        deconstruct_expanded_record(erh);
    }

    /* Tuple descriptor must be valid by now */
    tupdesc = (*erh).er_tupdesc;
    Assert!((*erh).nfields == (*tupdesc).natts);

    /* Flattened value will no longer represent record accurately */
    (*erh).flags &= !ER_FLAG_FVALUE_VALID;
    /* And we don't know the flattened size either */
    (*erh).flat_size = 0;

    oldcxt = MemoryContextSwitchTo((*erh).hdr.eoh_context);

    dvalues = (*erh).dvalues;
    dnulls = (*erh).dnulls;

    fnumber = 0;
    while fnumber < (*erh).nfields {
        let attr: *mut CompactAttribute = TupleDescCompactAttr(tupdesc, fnumber);
        let mut newValue: Datum;
        let isnull: bool;

        /* Ignore dropped columns */
        if (*attr).attisdropped {
            fnumber += 1;
            continue;
        }

        newValue = *newValues.add(fnumber as usize);
        isnull = *isnulls.add(fnumber as usize);

        if !(*attr).attbyval {
            /*
             * Copy new field value into record's context, and deal with
             * detoasting, if needed.
             */
            if !isnull {
                /* Is it an external toasted value? */
                if (*attr).attlen == -1
                    && VARATT_IS_EXTERNAL(DatumGetPointer(newValue))
                {
                    if expand_external {
                        /* Detoast as requested while copying the value */
                        newValue = PointerGetDatum(detoast_external_attr(
                            DatumGetPointer(newValue) as *mut varlena,
                        ) as *const c_void);
                    } else {
                        /* Just copy the value */
                        newValue = datumCopy(newValue, false, -1);
                        /* If it's still external, remember that */
                        if VARATT_IS_EXTERNAL(DatumGetPointer(newValue)) {
                            (*erh).flags |= ER_FLAG_HAVE_EXTERNAL;
                        }
                    }
                } else {
                    /* Not an external value, just copy it */
                    newValue = datumCopy(newValue, false, (*attr).attlen as c_int);
                }

                /* Remember that we have field(s) that need to be pfree'd */
                (*erh).flags |= ER_FLAG_DVALUES_ALLOCED;
            }

            /*
             * Free old field value, if any (not likely, since really we ought
             * to be inserting into an empty record).
             */
            if unlikely(!*dnulls.add(fnumber as usize)) {
                let oldValue: *mut c_char;

                oldValue = DatumGetPointer(*dvalues.add(fnumber as usize)) as *mut c_char;
                /* Don't try to pfree a part of the original flat record */
                if oldValue < (*erh).fstartptr || oldValue >= (*erh).fendptr {
                    pfree(oldValue as *mut c_void);
                }
            }
        }

        /* And finally we can insert the new field. */
        *dvalues.add(fnumber as usize) = newValue;
        *dnulls.add(fnumber as usize) = isnull;

        fnumber += 1;
    }

    /*
     * Because we don't guarantee atomicity of set_fields(), we can just leave
     * checking of domain constraints to occur as the final step; if it throws
     * an error, too bad.
     */
    if ((*erh).flags & ER_FLAG_IS_DOMAIN) != 0 {
        /* We run domain_check in a short-lived context to limit cruft */
        MemoryContextSwitchTo(get_short_term_cxt(erh));

        domain_check(
            ExpandedRecordGetRODatum(erh),
            false,
            (*erh).er_decltypeid,
            &mut (*erh).er_domaininfo,
            (*erh).hdr.eoh_context,
        );
    }

    MemoryContextSwitchTo(oldcxt);
}

/// Construct (or reset) working memory context for short-term operations.
///
/// This context is used for domain check evaluation and for detoasting.
///
/// If we don't have a short-lived memory context, make one; if we have one,
/// reset it to get rid of any leftover cruft.
unsafe fn get_short_term_cxt(erh: *mut ExpandedRecordHeader) -> MemoryContext {
    if (*erh).er_short_term_cxt.is_null() {
        (*erh).er_short_term_cxt = AllocSetContextCreateFn(
            (*erh).hdr.eoh_context,
            b"expanded record short-term context\0".as_ptr() as *const c_char,
            ALLOCSET_SMALL_SIZES,
        );
    } else {
        MemoryContextReset((*erh).er_short_term_cxt);
    }
    (*erh).er_short_term_cxt
}

/// Construct "dummy header" for checking domain constraints.
///
/// Since we don't want to modify the state of the expanded record until
/// we've validated the constraints, our approach is to set up a dummy
/// record header containing the new field value(s) and then pass that to
/// domain_check.  We retain the dummy header as part of the expanded
/// record's state to save palloc cycles, but reinitialize (most of)
/// its contents on each use.
unsafe fn build_dummy_expanded_header(main_erh: *mut ExpandedRecordHeader) {
    let mut erh: *mut ExpandedRecordHeader;
    let tupdesc: TupleDesc = expanded_record_get_tupdesc(main_erh);

    /* Ensure we have a short-lived context */
    let _ = get_short_term_cxt(main_erh);

    /*
     * Allocate dummy header on first time through, or in the unlikely event
     * that the number of fields changes (in which case we just leak the old
     * one).
     */
    erh = (*main_erh).er_dummy_header;
    if erh.is_null() || (*erh).nfields != (*tupdesc).natts {
        let chunk: *mut c_char;

        erh = MemoryContextAlloc(
            (*main_erh).hdr.eoh_context,
            MAXALIGN(core::mem::size_of::<ExpandedRecordHeader>())
                + (*tupdesc).natts as usize
                    * (core::mem::size_of::<Datum>() + core::mem::size_of::<bool>()),
        ) as *mut ExpandedRecordHeader;

        /* Ensure all header fields are initialized to 0/null */
        memset(
            erh as *mut c_void,
            0,
            core::mem::size_of::<ExpandedRecordHeader>(),
        );

        /*
         * We set up the dummy header with an indication that its memory
         * context is the short-lived context.
         */
        EOH_init_header(&mut (*erh).hdr, &ER_methods, (*main_erh).er_short_term_cxt);
        (*erh).er_magic = ER_MAGIC;

        /* Set up dvalues/dnulls, with no valid contents as yet */
        chunk = (erh as *mut c_char).add(MAXALIGN(core::mem::size_of::<ExpandedRecordHeader>()));
        (*erh).dvalues = chunk as *mut Datum;
        (*erh).dnulls =
            chunk.add((*tupdesc).natts as usize * core::mem::size_of::<Datum>()) as *mut bool;
        (*erh).nfields = (*tupdesc).natts;

        /*
         * The fields we just set are assumed to remain constant through
         * multiple uses of the dummy header to check domain constraints.
         */

        (*main_erh).er_dummy_header = erh;
    }

    /*
     * If anything inquires about the dummy header's declared type, it should
     * report the composite base type, not the domain type.  Hence we do not
     * transfer over the IS_DOMAIN flag.  But don't forget to mark header as
     * dummy.
     */
    (*erh).flags = ER_FLAG_IS_DUMMY;

    /* Copy composite-type identification info */
    (*erh).er_typeid = (*main_erh).er_typeid;
    (*erh).er_decltypeid = (*erh).er_typeid;
    (*erh).er_typmod = (*main_erh).er_typmod;

    /* Dummy header does not need its own tupdesc refcount */
    (*erh).er_tupdesc = tupdesc;
    (*erh).er_tupdesc_id = (*main_erh).er_tupdesc_id;

    /*
     * It's tempting to copy over whatever we know about the flat size, but
     * there's no point since we're surely about to modify the dummy record's
     * field(s).  Instead just clear anything left over from a previous usage
     * cycle.
     */
    (*erh).flat_size = 0;

    /* Copy over fvalue if we have it, so that system columns are available */
    (*erh).fvalue = (*main_erh).fvalue;
    (*erh).fstartptr = (*main_erh).fstartptr;
    (*erh).fendptr = (*main_erh).fendptr;
}

/// Precheck domain constraints for a set_field operation
#[inline(never)]
unsafe fn check_domain_for_new_field(
    erh: *mut ExpandedRecordHeader,
    fnumber: c_int,
    newValue: Datum,
    isnull: bool,
) {
    let dummy_erh: *mut ExpandedRecordHeader;
    let oldcxt: MemoryContext;

    /* Construct dummy header to contain proposed new field set */
    build_dummy_expanded_header(erh);
    dummy_erh = (*erh).er_dummy_header;

    /*
     * If record isn't empty, just deconstruct it (if needed) and copy over
     * the existing field values.  If it is empty, just fill fields with nulls
     * manually --- don't call deconstruct_expanded_record prematurely.
     */
    if !ExpandedRecordIsEmpty(erh) {
        deconstruct_expanded_record(erh);
        memcpy(
            (*dummy_erh).dvalues as *mut c_void,
            (*erh).dvalues as *const c_void,
            (*dummy_erh).nfields as usize * core::mem::size_of::<Datum>(),
        );
        memcpy(
            (*dummy_erh).dnulls as *mut c_void,
            (*erh).dnulls as *const c_void,
            (*dummy_erh).nfields as usize * core::mem::size_of::<bool>(),
        );
        /* There might be some external values in there... */
        (*dummy_erh).flags |= (*erh).flags & ER_FLAG_HAVE_EXTERNAL;
    } else {
        memset(
            (*dummy_erh).dvalues as *mut c_void,
            0,
            (*dummy_erh).nfields as usize * core::mem::size_of::<Datum>(),
        );
        memset(
            (*dummy_erh).dnulls as *mut c_void,
            true as c_int,
            (*dummy_erh).nfields as usize * core::mem::size_of::<bool>(),
        );
    }

    /* Either way, we now have valid dvalues */
    (*dummy_erh).flags |= ER_FLAG_DVALUES_VALID;

    /* Caller error if fnumber is system column or nonexistent column */
    if unlikely(fnumber <= 0 || fnumber > (*dummy_erh).nfields) {
        elog!(ERROR, "cannot assign to field {} of expanded record", fnumber);
    }

    /* Insert proposed new value into dummy field array */
    *(*dummy_erh).dvalues.add((fnumber - 1) as usize) = newValue;
    *(*dummy_erh).dnulls.add((fnumber - 1) as usize) = isnull;

    /*
     * The proposed new value might be external, in which case we'd better set
     * the flag for that in dummy_erh.
     */
    if !isnull {
        let attr: *mut CompactAttribute = TupleDescCompactAttr((*erh).er_tupdesc, fnumber - 1);

        if !(*attr).attbyval
            && (*attr).attlen == -1
            && VARATT_IS_EXTERNAL(DatumGetPointer(newValue))
        {
            (*dummy_erh).flags |= ER_FLAG_HAVE_EXTERNAL;
        }
    }

    /*
     * We call domain_check in the short-lived context, so that any cruft
     * leaked by expression evaluation can be reclaimed.
     */
    oldcxt = MemoryContextSwitchTo((*erh).er_short_term_cxt);

    /*
     * And now we can apply the check.  Note we use main header's domain cache
     * space, so that caching carries across repeated uses.
     */
    domain_check(
        ExpandedRecordGetRODatum(dummy_erh),
        false,
        (*erh).er_decltypeid,
        &mut (*erh).er_domaininfo,
        (*erh).hdr.eoh_context,
    );

    MemoryContextSwitchTo(oldcxt);

    /* We might as well clean up cruft immediately. */
    MemoryContextReset((*erh).er_short_term_cxt);
}

/// Precheck domain constraints for a set_tuple operation
#[inline(never)]
unsafe fn check_domain_for_new_tuple(erh: *mut ExpandedRecordHeader, tuple: HeapTuple) {
    let dummy_erh: *mut ExpandedRecordHeader;
    let oldcxt: MemoryContext;

    /* If we're being told to set record to empty, just see if NULL is OK */
    if tuple.is_null() {
        /* We run domain_check in a short-lived context to limit cruft */
        oldcxt = MemoryContextSwitchTo(get_short_term_cxt(erh));

        domain_check(
            0 as Datum,
            true,
            (*erh).er_decltypeid,
            &mut (*erh).er_domaininfo,
            (*erh).hdr.eoh_context,
        );

        MemoryContextSwitchTo(oldcxt);

        /* We might as well clean up cruft immediately. */
        MemoryContextReset((*erh).er_short_term_cxt);

        return;
    }

    /* Construct dummy header to contain replacement tuple */
    build_dummy_expanded_header(erh);
    dummy_erh = (*erh).er_dummy_header;

    /* Insert tuple, but don't bother to deconstruct its fields for now */
    (*dummy_erh).fvalue = tuple;
    (*dummy_erh).fstartptr = (*tuple).t_data as *mut c_char;
    (*dummy_erh).fendptr = ((*tuple).t_data as *mut c_char).add((*tuple).t_len as usize);
    (*dummy_erh).flags |= ER_FLAG_FVALUE_VALID;

    /* Remember if we have any out-of-line field values */
    if HeapTupleHasExternal(tuple) {
        (*dummy_erh).flags |= ER_FLAG_HAVE_EXTERNAL;
    }

    /*
     * We call domain_check in the short-lived context, so that any cruft
     * leaked by expression evaluation can be reclaimed.
     */
    oldcxt = MemoryContextSwitchTo((*erh).er_short_term_cxt);

    /*
     * And now we can apply the check.  Note we use main header's domain cache
     * space, so that caching carries across repeated uses.
     */
    domain_check(
        ExpandedRecordGetRODatum(dummy_erh),
        false,
        (*erh).er_decltypeid,
        &mut (*erh).er_domaininfo,
        (*erh).hdr.eoh_context,
    );

    MemoryContextSwitchTo(oldcxt);

    /* We might as well clean up cruft immediately. */
    MemoryContextReset((*erh).er_short_term_cxt);
}
