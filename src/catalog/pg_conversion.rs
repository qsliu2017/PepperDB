//! Translation of postgres/src/include/catalog/pg_conversion.h
//!
//! The `FormData_pg_conversion` struct: the fixed-layout part of a
//! pg_conversion catalog row.  The C header has no `#ifdef CATALOG_VARLEN`
//! cutoff, so every column declared in the CATALOG(...) body is part of this
//! in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(clippy::too_many_arguments)]

use crate::c::{int32, NameData};
use crate::catalog::catalog_oids::{
    ConversionRelationId, NamespaceRelationId, ProcedureRelationId,
};
use crate::common::encnames::pg_encoding_to_char;
use crate::postgres::{Datum, Int32GetDatum};
use crate::postgres_ext::Oid;
use crate::{elog, ereport, errmsg};
use core::ffi::{c_char, c_int, c_void};

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_conversion - the fixed part of a pg_conversion row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_conversion {
    /* oid */
    pub oid: Oid,
    /* name of the conversion */
    pub conname: NameData,
    /* namespace that the conversion belongs to */
    pub connamespace: Oid,
    /* owner of the conversion */
    pub conowner: Oid,
    /* FOR encoding id */
    pub conforencoding: int32,
    /* TO encoding id */
    pub contoencoding: int32,
    /* OID of the conversion proc */
    pub conproc: regproc,
    /* true if this is a default conversion */
    pub condefault: bool,
}

/*
 * Form_pg_conversion corresponds to a pointer to a tuple with the format of
 * the pg_conversion relation.
 */
pub type Form_pg_conversion = *mut FormData_pg_conversion;

// ===========================================================================
// Translation of postgres/src/backend/catalog/pg_conversion.c
//   routines to support manipulation of the pg_conversion relation
// ===========================================================================

/* log levels  TODO(pg-port): real values from utils/elog.h */
const ERROR: c_int = 21;

const InvalidOid: Oid = 0;

/* syscache ids  TODO(pg-port): utils/syscache.h */
const CONNAMENSP: c_int = 18;
const CONDEFAULT: c_int = 17;

/* Natts / Anum for pg_conversion  TODO(pg-port): catalog/pg_conversion.h */
const Natts_pg_conversion: usize = 8;
const Anum_pg_conversion_oid: c_int = 1;
const Anum_pg_conversion_conname: c_int = 2;
const Anum_pg_conversion_connamespace: c_int = 3;
const Anum_pg_conversion_conowner: c_int = 4;
const Anum_pg_conversion_conforencoding: c_int = 5;
const Anum_pg_conversion_contoencoding: c_int = 6;
const Anum_pg_conversion_conproc: c_int = 7;
const Anum_pg_conversion_condefault: c_int = 8;

/* index oid  TODO(pg-port): catalog/pg_conversion.h */
const ConversionOidIndexId: Oid = 2670;

/* lock modes  TODO(pg-port): storage/lockdefs.h */
const RowExclusiveLock: c_int = 3;

/* dependency type  TODO(pg-port): catalog/dependency.h */
const DEPENDENCY_NORMAL: c_int = b'n' as c_int;

/* ----------------------------------------------------------------
 * Local type aliases / forward declarations for unported deps.
 * ---------------------------------------------------------------- */

/* HeapTuple  TODO(pg-port): access/htup.h */
type HeapTuple = *mut c_void;
/* HeapTupleData  TODO(pg-port): access/htup.h */
type HeapTupleData = c_void;
/* Relation  TODO(pg-port): utils/rel.h */
type Relation = *mut RelationData;
/* TupleDesc  TODO(pg-port): access/tupdesc.h */
type TupleDesc = *mut c_void;
/* AttrNumber  TODO(pg-port): access/attnum.h */
type AttrNumber = i16;

#[repr(C)]
pub struct RelationData {
    pub rd_att: TupleDesc,
}

#[repr(C)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: c_int,
}

/* catcache list  TODO(pg-port): utils/catcache.h */
#[repr(C)]
pub struct CatCList {
    pub n_members: c_int,
    pub members: *mut *mut CatCTup,
}
#[repr(C)]
pub struct CatCTup {
    pub tuple: HeapTupleData,
}

/* Datum conversion  TODO(pg-port): postgres.h */
fn ObjectIdGetDatum(oid: Oid) -> Datum {
    oid as Datum
}
fn BoolGetDatum(b: bool) -> Datum {
    b as Datum
}
fn PointerGetDatum(p: *const c_void) -> Datum {
    p as Datum
}
unsafe fn NameGetDatum(name: *const NameData) -> Datum {
    name as Datum
}

/* syscache  TODO(pg-port): utils/syscache.h */
unsafe fn SearchSysCacheExists2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> bool { crate::utils::cache::syscache::SearchSysCacheExists2(_cacheId as _, _key1 as _, _key2 as _) }
unsafe fn SearchSysCacheList3(
    _cacheId: c_int,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
) -> *mut CatCList {
    core::ptr::null_mut()
}
unsafe fn ReleaseSysCacheList(_list: *mut CatCList) {}

/* access/htup_details.h  TODO(pg-port) */
unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    core::ptr::null_mut()
}

/* table am  TODO(pg-port): access/table.h, access/heapam.h */
unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    core::ptr::null_mut()
}
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {}
unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple { unimplemented!() }
unsafe fn heap_freetuple(_htup: HeapTuple) { crate::access::common::heaptuple::heap_freetuple(_htup as _) }

/* catalog/indexing.h, catalog/catalog.h  TODO(pg-port) */
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {}
unsafe fn GetNewOidWithIndex(
    _relation: Relation,
    _indexId: Oid,
    _oidcolumn: AttrNumber,
) -> Oid {
    InvalidOid
}

/* utils/adt/name.c  TODO(pg-port) */
unsafe fn namestrcpy(_name: *mut NameData, _s: *const c_char) -> c_int {
    0
}

/* catalog/dependency.h, catalog/pg_shdepend.h  TODO(pg-port) */
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_int,
) { crate::catalog::pg_depend::recordDependencyOn(_depender as _, _referenced as _, _behavior as _) }
unsafe fn recordDependencyOnOwner(_classId: Oid, _objectId: Oid, _owner: Oid) {}
unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _isReplace: bool) {}

/* catalog/objectaccess.h  TODO(pg-port) */
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {}

/*
 * ConversionCreate
 *
 * Add a new tuple to pg_conversion.
 */
pub unsafe fn ConversionCreate(
    conname: *const c_char,
    connamespace: Oid,
    conowner: Oid,
    conforencoding: int32,
    contoencoding: int32,
    conproc: Oid,
    def: bool,
) -> ObjectAddress {
    let mut i: c_int;
    let rel: Relation;
    let tupDesc: TupleDesc;
    let tup: HeapTuple;
    let oid: Oid;
    let mut nulls: [bool; Natts_pg_conversion] = [false; Natts_pg_conversion];
    let mut values: [Datum; Natts_pg_conversion] = [0 as Datum; Natts_pg_conversion];
    let mut cname: NameData = core::mem::zeroed();
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    /* sanity checks */
    if conname.is_null() {
        elog!(ERROR, "no conversion name supplied");
    }

    /* make sure there is no existing conversion of same name */
    if SearchSysCacheExists2(
        CONNAMENSP,
        PointerGetDatum(conname as *const c_void),
        ObjectIdGetDatum(connamespace),
    ) {
        ereport!(
            ERROR,
            errmsg!(
                "conversion \"{}\" already exists",
                std::ffi::CStr::from_ptr(conname).to_string_lossy()
            )
        ); /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }

    if def {
        /*
         * make sure there is no existing default <for encoding><to encoding>
         * pair in this name space
         */
        if FindDefaultConversion(connamespace, conforencoding, contoencoding) != InvalidOid {
            ereport!(
                ERROR,
                errmsg!(
                    "default conversion for {} to {} already exists",
                    std::ffi::CStr::from_ptr(pg_encoding_to_char(conforencoding))
                        .to_string_lossy(),
                    std::ffi::CStr::from_ptr(pg_encoding_to_char(contoencoding))
                        .to_string_lossy()
                )
            ); /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    /* open pg_conversion */
    rel = table_open(ConversionRelationId, RowExclusiveLock);
    tupDesc = (*rel).rd_att;

    /* initialize nulls and values */
    i = 0;
    while (i as usize) < Natts_pg_conversion {
        nulls[i as usize] = false;
        values[i as usize] = 0 as Datum;
        i += 1;
    }

    /* form a tuple */
    namestrcpy(&mut cname, conname);
    oid = GetNewOidWithIndex(rel, ConversionOidIndexId, Anum_pg_conversion_oid as AttrNumber);
    values[(Anum_pg_conversion_oid - 1) as usize] = ObjectIdGetDatum(oid);
    values[(Anum_pg_conversion_conname - 1) as usize] = NameGetDatum(&cname);
    values[(Anum_pg_conversion_connamespace - 1) as usize] = ObjectIdGetDatum(connamespace);
    values[(Anum_pg_conversion_conowner - 1) as usize] = ObjectIdGetDatum(conowner);
    values[(Anum_pg_conversion_conforencoding - 1) as usize] = Int32GetDatum(conforencoding);
    values[(Anum_pg_conversion_contoencoding - 1) as usize] = Int32GetDatum(contoencoding);
    values[(Anum_pg_conversion_conproc - 1) as usize] = ObjectIdGetDatum(conproc);
    values[(Anum_pg_conversion_condefault - 1) as usize] = BoolGetDatum(def);

    tup = heap_form_tuple(tupDesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    /* insert a new tuple */
    CatalogTupleInsert(rel, tup);

    myself.classId = ConversionRelationId;
    myself.objectId = oid;
    myself.objectSubId = 0;

    /* create dependency on conversion procedure */
    referenced.classId = ProcedureRelationId;
    referenced.objectId = conproc;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* create dependency on namespace */
    referenced.classId = NamespaceRelationId;
    referenced.objectId = connamespace;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* create dependency on owner */
    recordDependencyOnOwner(ConversionRelationId, oid, conowner);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new conversion */
    InvokeObjectPostCreateHook(ConversionRelationId, oid, 0);

    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);

    myself
}

/*
 * FindDefaultConversion
 *
 * Find "default" conversion proc by for_encoding and to_encoding in the
 * given namespace.
 *
 * If found, returns the procedure's oid, otherwise InvalidOid.  Note that
 * you get the procedure's OID not the conversion's OID!
 */
pub unsafe fn FindDefaultConversion(
    name_space: Oid,
    for_encoding: int32,
    to_encoding: int32,
) -> Oid {
    let catlist: *mut CatCList;
    let mut tuple: HeapTuple;
    let mut body: Form_pg_conversion;
    let mut proc: Oid = InvalidOid;
    let mut i: c_int;

    catlist = SearchSysCacheList3(
        CONDEFAULT,
        ObjectIdGetDatum(name_space),
        Int32GetDatum(for_encoding),
        Int32GetDatum(to_encoding),
    );

    i = 0;
    while i < (*catlist).n_members {
        tuple = &mut (**(*catlist).members.offset(i as isize)).tuple as *mut _ as HeapTuple;
        body = GETSTRUCT(tuple) as Form_pg_conversion;
        if (*body).condefault {
            proc = (*body).conproc;
            break;
        }
        i += 1;
    }
    ReleaseSysCacheList(catlist);
    proc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // conname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_conversion, conname), 4);
        // connamespace follows the NAMEDATALEN-byte conname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_conversion, connamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_conversion>()
                >= core::mem::offset_of!(FormData_pg_conversion, condefault)
                    + core::mem::size_of::<bool>()
        );
    }
}
