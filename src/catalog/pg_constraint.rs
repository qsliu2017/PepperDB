//! Translation of postgres/src/include/catalog/pg_constraint.h
//!
//! The `FormData_pg_constraint` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_constraint catalog row.  This is exactly the portion of the row
//! that the C struct exposes in memory; the variable-length / nullable trailing
//! fields (conkey, confkey, conpfeqop, conppeqop, conffeqop, confdelsetcols,
//! conexclop, conbin, guarded by CATALOG_VARLEN in the C header) are NOT part of
//! this struct - they live only in a real on-disk pg_constraint tuple and are
//! reached via heap_getattr.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int16, NameData};
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/*
 * FormData_pg_constraint - the fixed part of a pg_constraint row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_constraint {
    /* oid */
    pub oid: Oid,
    /* name of this constraint */
    pub conname: NameData,
    /* OID of namespace containing constraint */
    pub connamespace: Oid,
    /* constraint type; see CONSTRAINT_* codes below */
    pub contype: c_char,
    /* deferrable constraint? */
    pub condeferrable: bool,
    /* deferred by default? */
    pub condeferred: bool,
    /* enforced constraint? */
    pub conenforced: bool,
    /* constraint has been validated? */
    pub convalidated: bool,
    /* relation this constraint constrains; 0 if not relation-specific */
    pub conrelid: Oid,
    /* domain this constraint constrains; 0 if not a domain constraint */
    pub contypid: Oid,
    /* index supporting this constraint, if any; else 0 */
    pub conindid: Oid,
    /* corresponding constraint OID in parent if inherited partition; else 0 */
    pub conparentid: Oid,
    /* relation referenced by foreign key; 0 if not a foreign key */
    pub confrelid: Oid,
    /* foreign key's ON UPDATE action */
    pub confupdtype: c_char,
    /* foreign key's ON DELETE action */
    pub confdeltype: c_char,
    /* foreign key's match type */
    pub confmatchtype: c_char,
    /* has a local definition (do not drop when coninhcount is 0) */
    pub conislocal: bool,
    /* number of times inherited from direct parent relation(s) */
    pub coninhcount: int16,
    /* has a local definition and cannot be inherited */
    pub connoinherit: bool,
    /* last column uses overlaps instead of equals (PK/unique/FK) */
    pub conperiod: bool,
}

/*
 * Form_pg_constraint corresponds to a pointer to a row with the format of the
 * pg_constraint relation.
 */
pub type Form_pg_constraint = *mut FormData_pg_constraint;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 * ----------------------------------------------------------------
 */

/* Valid values for contype */
pub const CONSTRAINT_CHECK: c_char = b'c' as c_char;
pub const CONSTRAINT_FOREIGN: c_char = b'f' as c_char;
pub const CONSTRAINT_NOTNULL: c_char = b'n' as c_char;
pub const CONSTRAINT_PRIMARY: c_char = b'p' as c_char;
pub const CONSTRAINT_UNIQUE: c_char = b'u' as c_char;
pub const CONSTRAINT_TRIGGER: c_char = b't' as c_char;
pub const CONSTRAINT_EXCLUSION: c_char = b'x' as c_char;

/*
 * Valid values for confupdtype and confdeltype are the FKCONSTR_ACTION_xxx
 * constants defined in parsenodes.h.  Valid values for confmatchtype are the
 * FKCONSTR_MATCH_xxx constants defined in parsenodes.h.
 */

/* ----------------------------------------------------------------
 * routines to support manipulation of the pg_constraint relation
 * (translated from src/backend/catalog/pg_constraint.c)
 * ----------------------------------------------------------------
 */

use crate::prelude::*;
use crate::{foreach, IsA};
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::stratnum::StrategyNumber;
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{heap_getattr, GETSTRUCT, HeapTuple, HeapTupleIsValid};
use crate::access::common::heaptuple::heap_copytuple;
use crate::access::index::genam::{
    systable_beginscan, systable_endscan, systable_getnext, SysScanDesc,
};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog_oids::ConstraintRelationId;
use crate::catalog::pg_type_d::INT2OID;
use crate::nodes::bitmapset::{bms_add_member, bms_is_subset, Bitmapset};
use crate::nodes::pg_list::{lappend_oid, lfirst, List};
use crate::nodes::primnodes::Var;
use crate::storage::lockdefs::{AccessShareLock, LOCKMODE};
use crate::utils::array::{ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_NDIM, ArrayType};
use crate::utils::cache::lsyscache::{get_attnum, get_rel_name};
use crate::utils::cache::syscache::SysCacheGetAttrNotNull;
use crate::utils::rel::{Relation, RelationGetDescr, RelationGetRelationName};
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::nodes::pg_list::lappend;
use crate::nodes::value::makeString;
use crate::nodes::parsenodes::{Constraint, CONSTR_NOTNULL};
use crate::nodes::nodes::Node;
use crate::catalog::heap::CookedConstraint;
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::dependency::{object_address_present, ObjectAddresses};
use crate::catalog::catalog_oids::{
    RelationRelationId, TypeRelationId, OperatorRelationId,
};
use crate::catalog::pg_class::FormData_pg_class;
use crate::storage::lockdefs::{RowExclusiveLock, AccessExclusiveLock, NoLock};
use crate::common::int::pg_add_s16_overflow;
use crate::pg_config_manual::{INDEX_MAX_KEYS, NAMEDATALEN};
use crate::access::cmptype::COMPARE_CONTAINED_BY;
use crate::catalog::pg_known_oids::OID_RANGE_INTERSECT_RANGE_OP;
use crate::commands::indexcmds::GetOperatorFromCompareType;
use crate::c::Pointer;
use crate::makeNode;

type Form_pg_class = *mut FormData_pg_class;

/* Dependency categories (dependency.h) */
const DEPENDENCY_NORMAL: c_char = b'n' as c_char;
const DEPENDENCY_AUTO: c_char = b'a' as c_char;
const DEPENDENCY_PARTITION_PRI: c_char = b'P' as c_char;
const DEPENDENCY_PARTITION_SEC: c_char = b'S' as c_char;

/*
 * ConstraintCategory - argument to ConstraintNameIsUsed (pg_constraint.h).
 */
#[allow(non_camel_case_types)]
pub type ConstraintCategory = c_int;
pub const CONSTRAINT_RELATION: ConstraintCategory = 0;
pub const CONSTRAINT_DOMAIN: ConstraintCategory = 1;

/* syscache ids (syscache.h) */
const RELOID: c_int = 56;

/* type OIDs referenced by FindFKPeriodOpers (pg_type.h) */
const ANYRANGEOID: Oid = 3831;
const ANYMULTIRANGEOID: Oid = 4537;
const OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP: Oid = 4848;

/* number of attributes in pg_constraint (pg_constraint.h) */
const Natts_pg_constraint: usize = 33;

/* additional pg_constraint index OIDs and Anum constants (pg_constraint.h) */
const ConstraintOidIndexId: Oid = 2667;
const ConstraintNameNspIndexId: Oid = 2664;

const Anum_pg_constraint_contype: AttrNumber = 4;
const Anum_pg_constraint_condeferrable: AttrNumber = 5;
const Anum_pg_constraint_condeferred: AttrNumber = 6;
const Anum_pg_constraint_conenforced: AttrNumber = 7;
const Anum_pg_constraint_convalidated: AttrNumber = 8;
const Anum_pg_constraint_conindid: AttrNumber = 11;
const Anum_pg_constraint_conparentid: AttrNumber = 12;
const Anum_pg_constraint_confrelid: AttrNumber = 13;
const Anum_pg_constraint_confupdtype: AttrNumber = 14;
const Anum_pg_constraint_confdeltype: AttrNumber = 15;
const Anum_pg_constraint_confmatchtype: AttrNumber = 16;
const Anum_pg_constraint_conislocal: AttrNumber = 17;
const Anum_pg_constraint_coninhcount: AttrNumber = 18;
const Anum_pg_constraint_connoinherit: AttrNumber = 19;
const Anum_pg_constraint_conperiod: AttrNumber = 20;
const Anum_pg_constraint_confkey: AttrNumber = 22;
const Anum_pg_constraint_conpfeqop: AttrNumber = 23;
const Anum_pg_constraint_conppeqop: AttrNumber = 24;
const Anum_pg_constraint_conffeqop: AttrNumber = 25;
const Anum_pg_constraint_confdelsetcols: AttrNumber = 26;
const Anum_pg_constraint_conexclop: AttrNumber = 27;
const Anum_pg_constraint_conbin: AttrNumber = 28;

/* OID element type for catalog arrays (pg_type.h) */
const OIDOID: Oid = 26;

/// Render a C string pointer for diagnostics (see %s -> {} convention).
unsafe fn CStr_to_display(s: *const c_char) -> std::borrow::Cow<'static, str> {
    if s.is_null() {
        std::borrow::Cow::Borrowed("(null)")
    } else {
        std::ffi::CStr::from_ptr(s).to_string_lossy()
    }
}

/*
 * Index OIDs and attribute numbers for pg_constraint.  Defined locally, as is
 * the convention for catalog Anum/index constants in the port (see the same
 * pattern in catalog/heap.rs); the values come from pg_constraint.h.
 */
const ConstraintRelidTypidNameIndexId: Oid = 2665;

const Anum_pg_constraint_oid: AttrNumber = 1;
const Anum_pg_constraint_conname: AttrNumber = 2;
const Anum_pg_constraint_connamespace: AttrNumber = 3;
const Anum_pg_constraint_conrelid: AttrNumber = 9;
const Anum_pg_constraint_contypid: AttrNumber = 10;
const Anum_pg_constraint_conkey: AttrNumber = 21;

/* btree strategy + equality procedure OIDs (stratnum.h / fmgroids.h) */
const BTEqualStrategyNumber: StrategyNumber = 3;
const F_OIDEQ: RegProcedure = 184;
const F_NAMEEQ: RegProcedure = 60;

/* syscache id for pg_constraint by OID (syscache.h) */
const CONSTROID: c_int = 25;

/*
 * Given a pg_constraint tuple for a not-null constraint, return the column
 * number it is for.
 */
pub unsafe fn extractNotNullColumn(constrTup: HeapTuple) -> AttrNumber {
    let adatum: Datum;
    let arr: *mut ArrayType;

    /* only tuples for not-null constraints should be given */
    Assert!(
        (*(GETSTRUCT(constrTup) as Form_pg_constraint)).contype == CONSTRAINT_NOTNULL
    );

    adatum = SysCacheGetAttrNotNull(CONSTROID, constrTup, Anum_pg_constraint_conkey);
    arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
    if ARR_NDIM(arr) != 1
        || ARR_HASNULL(arr)
        || ARR_ELEMTYPE(arr) != INT2OID
        || *ARR_DIMS(arr).add(0) != 1
    {
        elog!(ERROR, "conkey is not a 1-D smallint array");
    }

    /* We leak the detoasted datum, but we don't care */

    *(ARR_DATA_PTR(arr) as *mut AttrNumber).add(0)
}

/*
 * Find and return a copy of the pg_constraint tuple that implements a
 * (possibly not valid) not-null constraint for the given column of the
 * given relation.  If no such constraint exists, return NULL.
 *
 * XXX This would be easier if we had pg_attribute.notnullconstr with the OID
 * of the constraint that implements the not-null constraint for that column.
 * I'm not sure it's worth the catalog bloat and de-normalization, however.
 */
pub unsafe fn findNotNullConstraintAttnum(relid: Oid, attnum: AttrNumber) -> HeapTuple {
    let pg_constraint: Relation;
    let mut conTup: HeapTuple;
    let mut retval: HeapTuple = null_mut();
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    scan = systable_beginscan(
        pg_constraint,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        1,
        &mut key,
    );

    loop {
        conTup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(conTup) {
            break;
        }
        let con = GETSTRUCT(conTup) as Form_pg_constraint;
        let conkey: AttrNumber;

        /*
         * We're looking for a NOTNULL constraint with the column we're
         * looking for as the sole element in conkey.
         */
        if (*con).contype != CONSTRAINT_NOTNULL {
            continue;
        }

        conkey = extractNotNullColumn(conTup);
        if conkey != attnum {
            continue;
        }

        /* Found it */
        retval = heap_copytuple(conTup);
        break;
    }

    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    retval
}

/*
 * Find and return a copy of the pg_constraint tuple that implements a
 * (possibly not valid) not-null constraint for the given column of the
 * given relation.
 * If no such column or no such constraint exists, return NULL.
 */
pub unsafe fn findNotNullConstraint(relid: Oid, colname: *const c_char) -> HeapTuple {
    let attnum: AttrNumber;

    attnum = get_attnum(relid, colname);
    if attnum <= InvalidAttrNumber {
        return null_mut();
    }

    findNotNullConstraintAttnum(relid, attnum)
}

/*
 * Find and return the pg_constraint tuple that implements a validated
 * not-null constraint for the given domain.
 */
pub unsafe fn findDomainNotNullConstraint(typid: Oid) -> HeapTuple {
    let pg_constraint: Relation;
    let mut conTup: HeapTuple;
    let mut retval: HeapTuple = null_mut();
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(typid),
    );
    scan = systable_beginscan(
        pg_constraint,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        1,
        &mut key,
    );

    loop {
        conTup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(conTup) {
            break;
        }
        let con = GETSTRUCT(conTup) as Form_pg_constraint;

        /*
         * We're looking for a NOTNULL constraint that's marked validated.
         */
        if (*con).contype != CONSTRAINT_NOTNULL {
            continue;
        }
        if !(*con).convalidated {
            continue;
        }

        /* Found it */
        retval = heap_copytuple(conTup);
        break;
    }

    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    retval
}

/*
 * get_relation_constraint_oid
 *		Find a constraint on the specified relation with the specified name.
 *		Returns constraint's OID.
 */
pub unsafe fn get_relation_constraint_oid(
    relid: Oid,
    conname: *const c_char,
    missing_ok: bool,
) -> Oid {
    let pg_constraint: Relation;
    let tuple: HeapTuple;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 3] = core::mem::zeroed();
    let mut conOid: Oid = InvalidOid;

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(conname),
    );

    scan = systable_beginscan(
        pg_constraint,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    /* There can be at most one matching row */
    tuple = systable_getnext(scan) as HeapTuple;
    if HeapTupleIsValid(tuple) {
        conOid = (*(GETSTRUCT(tuple) as Form_pg_constraint)).oid;
    }

    systable_endscan(scan);

    /* If no such constraint exists, complain */
    if !OidIsValid(conOid) && !missing_ok {
        ereport!(
            ERROR,
            errmsg!(
                "constraint \"{}\" for table \"{}\" does not exist",
                CStr_to_display(conname),
                CStr_to_display(get_rel_name(relid))
            )
        );
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    table_close(pg_constraint, AccessShareLock);

    conOid
}

/*
 * get_relation_constraint_attnos
 *		Find a constraint on the specified relation with the specified name
 *		and return the constrained columns.
 *
 * Returns a Bitmapset of the column attnos of the constrained columns, with
 * attnos being offset by FirstLowInvalidHeapAttributeNumber so that system
 * columns can be represented.
 *
 * *constraintOid is set to the OID of the constraint, or InvalidOid on
 * failure.
 */
pub unsafe fn get_relation_constraint_attnos(
    relid: Oid,
    conname: *const c_char,
    missing_ok: bool,
    constraintOid: *mut Oid,
) -> *mut Bitmapset {
    let mut conattnos: *mut Bitmapset = null_mut();
    let pg_constraint: Relation;
    let tuple: HeapTuple;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 3] = core::mem::zeroed();

    /* Set *constraintOid, to avoid complaints about uninitialized vars */
    *constraintOid = InvalidOid;

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(conname),
    );

    scan = systable_beginscan(
        pg_constraint,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    /* There can be at most one matching row */
    tuple = systable_getnext(scan) as HeapTuple;
    if HeapTupleIsValid(tuple) {
        let adatum: Datum;
        let mut isNull: bool = false;

        *constraintOid = (*(GETSTRUCT(tuple) as Form_pg_constraint)).oid;

        /* Extract the conkey array, ie, attnums of constrained columns */
        adatum = heap_getattr(
            tuple,
            Anum_pg_constraint_conkey as c_int,
            RelationGetDescr(pg_constraint),
            &mut isNull,
        );
        if !isNull {
            let arr: *mut ArrayType;
            let numcols: c_int;
            let attnums: *mut int16;
            let mut i: c_int;

            arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
            numcols = *ARR_DIMS(arr).add(0);
            if ARR_NDIM(arr) != 1
                || numcols < 0
                || ARR_HASNULL(arr)
                || ARR_ELEMTYPE(arr) != INT2OID
            {
                elog!(ERROR, "conkey is not a 1-D smallint array");
            }
            attnums = ARR_DATA_PTR(arr) as *mut int16;

            /* Construct the result value */
            i = 0;
            while i < numcols {
                conattnos = bms_add_member(
                    conattnos,
                    *attnums.add(i as usize) as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
                );
                i += 1;
            }
        }
    }

    systable_endscan(scan);

    /* If no such constraint exists, complain */
    if !OidIsValid(*constraintOid) && !missing_ok {
        ereport!(
            ERROR,
            errmsg!(
                "constraint \"{}\" for table \"{}\" does not exist",
                CStr_to_display(conname),
                CStr_to_display(get_rel_name(relid))
            )
        );
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    table_close(pg_constraint, AccessShareLock);

    conattnos
}

/*
 * Return the OID of the constraint enforced by the given index in the
 * given relation; or InvalidOid if no such index is cataloged.
 *
 * Much like get_constraint_index, this function is concerned only with the
 * one constraint that "owns" the given index.  Therefore, constraints of
 * types other than unique, primary-key, and exclusion are ignored.
 */
pub unsafe fn get_relation_idx_constraint_oid(relationId: Oid, indexId: Oid) -> Oid {
    let pg_constraint: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut tuple: HeapTuple;
    let mut constraintId: Oid = InvalidOid;

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key,
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relationId),
    );
    scan = systable_beginscan(
        pg_constraint,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        1,
        &mut key,
    );
    loop {
        tuple = systable_getnext(scan) as HeapTuple;
        if tuple.is_null() {
            break;
        }
        let constrForm: Form_pg_constraint;

        constrForm = GETSTRUCT(tuple) as Form_pg_constraint;

        /* See above */
        if (*constrForm).contype != CONSTRAINT_PRIMARY
            && (*constrForm).contype != CONSTRAINT_UNIQUE
            && (*constrForm).contype != CONSTRAINT_EXCLUSION
        {
            continue;
        }

        if (*constrForm).conindid == indexId {
            constraintId = (*constrForm).oid;
            break;
        }
    }
    systable_endscan(scan);

    table_close(pg_constraint, AccessShareLock);
    constraintId
}

/*
 * get_domain_constraint_oid
 *		Find a constraint on the specified domain with the specified name.
 *		Returns constraint's OID.
 */
pub unsafe fn get_domain_constraint_oid(
    typid: Oid,
    conname: *const c_char,
    missing_ok: bool,
) -> Oid {
    let pg_constraint: Relation;
    let tuple: HeapTuple;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 3] = core::mem::zeroed();
    let mut conOid: Oid = InvalidOid;

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(typid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(conname),
    );

    scan = systable_beginscan(
        pg_constraint,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    /* There can be at most one matching row */
    tuple = systable_getnext(scan) as HeapTuple;
    if HeapTupleIsValid(tuple) {
        conOid = (*(GETSTRUCT(tuple) as Form_pg_constraint)).oid;
    }

    systable_endscan(scan);

    /* If no such constraint exists, complain */
    if !OidIsValid(conOid) && !missing_ok {
        ereport!(
            ERROR,
            errmsg!(
                "constraint \"{}\" for domain {} does not exist",
                CStr_to_display(conname),
                CStr_to_display(format_type_be(typid))
            )
        );
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    table_close(pg_constraint, AccessShareLock);

    conOid
}

/*
 * get_primary_key_attnos
 *		Identify the columns in a relation's primary key, if any.
 *
 * Returns a Bitmapset of the column attnos of the primary key's columns,
 * with attnos being offset by FirstLowInvalidHeapAttributeNumber so that
 * system columns can be represented.
 *
 * If there is no primary key, return NULL.  We also return NULL if the pkey
 * constraint is deferrable and deferrableOk is false.
 *
 * *constraintOid is set to the OID of the pkey constraint, or InvalidOid
 * on failure.
 */
pub unsafe fn get_primary_key_attnos(
    relid: Oid,
    deferrableOk: bool,
    constraintOid: *mut Oid,
) -> *mut Bitmapset {
    let mut pkattnos: *mut Bitmapset = null_mut();
    let pg_constraint: Relation;
    let mut tuple: HeapTuple;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 1] = core::mem::zeroed();

    /* Set *constraintOid, to avoid complaints about uninitialized vars */
    *constraintOid = InvalidOid;

    /* Scan pg_constraint for constraints of the target rel */
    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );

    scan = systable_beginscan(
        pg_constraint,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    loop {
        tuple = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let con = GETSTRUCT(tuple) as Form_pg_constraint;
        let adatum: Datum;
        let mut isNull: bool = false;
        let arr: *mut ArrayType;
        let attnums: *mut int16;
        let numkeys: c_int;
        let mut i: c_int;

        /* Skip constraints that are not PRIMARY KEYs */
        if (*con).contype != CONSTRAINT_PRIMARY {
            continue;
        }

        /*
         * If the primary key is deferrable, but we've been instructed to
         * ignore deferrable constraints, then we might as well give up
         * searching, since there can only be a single primary key on a table.
         */
        if (*con).condeferrable && !deferrableOk {
            break;
        }

        /* Extract the conkey array, ie, attnums of PK's columns */
        adatum = heap_getattr(
            tuple,
            Anum_pg_constraint_conkey as c_int,
            RelationGetDescr(pg_constraint),
            &mut isNull,
        );
        if isNull {
            elog!(
                ERROR,
                "null conkey for constraint {}",
                (*(GETSTRUCT(tuple) as Form_pg_constraint)).oid
            );
        }
        arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
        numkeys = *ARR_DIMS(arr).add(0);
        if ARR_NDIM(arr) != 1
            || numkeys < 0
            || ARR_HASNULL(arr)
            || ARR_ELEMTYPE(arr) != INT2OID
        {
            elog!(ERROR, "conkey is not a 1-D smallint array");
        }
        attnums = ARR_DATA_PTR(arr) as *mut int16;

        /* Construct the result value */
        i = 0;
        while i < numkeys {
            pkattnos = bms_add_member(
                pkattnos,
                *attnums.add(i as usize) as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
            );
            i += 1;
        }
        *constraintOid = (*(GETSTRUCT(tuple) as Form_pg_constraint)).oid;

        /* No need to search further */
        break;
    }

    systable_endscan(scan);

    table_close(pg_constraint, AccessShareLock);

    pkattnos
}

/*
 * Determine whether a relation can be proven functionally dependent on
 * a set of grouping columns.  If so, return true and add the pg_constraint
 * OIDs of the constraints needed for the proof to the *constraintDeps list.
 *
 * grouping_columns is a list of grouping expressions, in which columns of
 * the rel of interest are Vars with the indicated varno/varlevelsup.
 *
 * Currently we only check to see if the rel has a primary key that is a
 * subset of the grouping_columns.  We could also use plain unique constraints
 * if all their columns are known not null, but there's a problem: we need
 * to be able to represent the not-null-ness as part of the constraints added
 * to *constraintDeps.  FIXME whenever not-null constraints get represented
 * in pg_constraint.
 */
pub unsafe fn check_functional_grouping(
    relid: Oid,
    varno: Index,
    varlevelsup: Index,
    grouping_columns: *mut List,
    constraintDeps: *mut *mut List,
) -> bool {
    let pkattnos: *mut Bitmapset;
    let mut groupbyattnos: *mut Bitmapset;
    let mut constraintOid: Oid = InvalidOid;

    /* If the rel has no PK, then we can't prove functional dependency */
    pkattnos = get_primary_key_attnos(relid, false, &mut constraintOid);
    if pkattnos.is_null() {
        return false;
    }

    /* Identify all the rel's columns that appear in grouping_columns */
    groupbyattnos = null_mut();
    foreach!(gl, grouping_columns, {
        let gvar = lfirst(crate::current_cell!(gl)) as *mut Var;

        if IsA!(gvar, T_Var)
            && (*gvar).varno == varno as c_int
            && (*gvar).varlevelsup == varlevelsup
        {
            groupbyattnos = bms_add_member(
                groupbyattnos,
                (*gvar).varattno as c_int - FirstLowInvalidHeapAttributeNumber as c_int,
            );
        }
    });

    if bms_is_subset(pkattnos, groupbyattnos) {
        /* The PK is a subset of grouping_columns, so we win */
        *constraintDeps = lappend_oid(*constraintDeps, constraintOid);
        return true;
    }

    false
}

/*
 * CreateConstraintEntry
 *	Create a constraint table entry.
 *
 * Subsidiary records (such as triggers or indexes to implement the
 * constraint) are *not* created here.  But we do make dependency links
 * from the constraint to the things it depends on.
 *
 * The new constraint's OID is returned.
 */
pub unsafe fn CreateConstraintEntry(
    constraintName: *const c_char,
    constraintNamespace: Oid,
    constraintType: c_char,
    isDeferrable: bool,
    isDeferred: bool,
    isEnforced: bool,
    isValidated: bool,
    parentConstrId: Oid,
    relId: Oid,
    constraintKey: *const int16,
    constraintNKeys: c_int,
    constraintNTotalKeys: c_int,
    domainId: Oid,
    indexRelId: Oid,
    foreignRelId: Oid,
    foreignKey: *const int16,
    pfEqOp: *const Oid,
    ppEqOp: *const Oid,
    ffEqOp: *const Oid,
    foreignNKeys: c_int,
    foreignUpdateType: c_char,
    foreignDeleteType: c_char,
    fkDeleteSetCols: *const int16,
    numFkDeleteSetCols: c_int,
    foreignMatchType: c_char,
    exclOp: *const Oid,
    conExpr: *mut Node,
    conBin: *const c_char,
    conIsLocal: bool,
    conInhCount: int16,
    conNoInherit: bool,
    conPeriod: bool,
    is_internal: bool,
) -> Oid {
    let conDesc: Relation;
    let conOid: Oid;
    let tup: HeapTuple;
    let mut nulls: [bool; Natts_pg_constraint] = [false; Natts_pg_constraint];
    let mut values: [Datum; Natts_pg_constraint] = [0 as Datum; Natts_pg_constraint];
    let conkeyArray: *mut ArrayType;
    let confkeyArray: *mut ArrayType;
    let conpfeqopArray: *mut ArrayType;
    let conppeqopArray: *mut ArrayType;
    let conffeqopArray: *mut ArrayType;
    let conexclopArray: *mut ArrayType;
    let confdelsetcolsArray: *mut ArrayType;
    let mut cname: NameData = core::mem::zeroed();
    let mut i: c_int;
    let mut conobject: ObjectAddress = core::mem::zeroed();
    let addrs_auto: *mut ObjectAddresses;
    let addrs_normal: *mut ObjectAddresses;

    /* Only CHECK or FOREIGN KEY constraint can be not enforced */
    Assert!(isEnforced || constraintType == CONSTRAINT_CHECK || constraintType == CONSTRAINT_FOREIGN);
    /* NOT ENFORCED constraint must be NOT VALID */
    Assert!(isEnforced || !isValidated);

    conDesc = table_open(ConstraintRelationId, RowExclusiveLock);

    Assert!(!constraintName.is_null());
    namestrcpy(&mut cname, constraintName);

    /*
     * Convert C arrays into Postgres arrays.
     */
    if constraintNKeys > 0 {
        let conkey: *mut Datum;

        conkey = palloc(constraintNKeys as usize * core::mem::size_of::<Datum>()) as *mut Datum;
        i = 0;
        while i < constraintNKeys {
            *conkey.add(i as usize) = Int16GetDatum(*constraintKey.add(i as usize));
            i += 1;
        }
        conkeyArray = construct_array_builtin(conkey, constraintNKeys, INT2OID);
    } else {
        conkeyArray = null_mut();
    }

    if foreignNKeys > 0 {
        let fkdatums: *mut Datum;
        let nkeys = foreignNKeys.max(numFkDeleteSetCols);

        fkdatums = palloc(nkeys as usize * core::mem::size_of::<Datum>()) as *mut Datum;
        i = 0;
        while i < foreignNKeys {
            *fkdatums.add(i as usize) = Int16GetDatum(*foreignKey.add(i as usize));
            i += 1;
        }
        confkeyArray = construct_array_builtin(fkdatums, foreignNKeys, INT2OID);
        i = 0;
        while i < foreignNKeys {
            *fkdatums.add(i as usize) = ObjectIdGetDatum(*pfEqOp.add(i as usize));
            i += 1;
        }
        conpfeqopArray = construct_array_builtin(fkdatums, foreignNKeys, OIDOID);
        i = 0;
        while i < foreignNKeys {
            *fkdatums.add(i as usize) = ObjectIdGetDatum(*ppEqOp.add(i as usize));
            i += 1;
        }
        conppeqopArray = construct_array_builtin(fkdatums, foreignNKeys, OIDOID);
        i = 0;
        while i < foreignNKeys {
            *fkdatums.add(i as usize) = ObjectIdGetDatum(*ffEqOp.add(i as usize));
            i += 1;
        }
        conffeqopArray = construct_array_builtin(fkdatums, foreignNKeys, OIDOID);

        if numFkDeleteSetCols > 0 {
            i = 0;
            while i < numFkDeleteSetCols {
                *fkdatums.add(i as usize) = Int16GetDatum(*fkDeleteSetCols.add(i as usize));
                i += 1;
            }
            confdelsetcolsArray =
                construct_array_builtin(fkdatums, numFkDeleteSetCols, INT2OID);
        } else {
            confdelsetcolsArray = null_mut();
        }
    } else {
        confkeyArray = null_mut();
        conpfeqopArray = null_mut();
        conppeqopArray = null_mut();
        conffeqopArray = null_mut();
        confdelsetcolsArray = null_mut();
    }

    if !exclOp.is_null() {
        let opdatums: *mut Datum;

        opdatums = palloc(constraintNKeys as usize * core::mem::size_of::<Datum>()) as *mut Datum;
        i = 0;
        while i < constraintNKeys {
            *opdatums.add(i as usize) = ObjectIdGetDatum(*exclOp.add(i as usize));
            i += 1;
        }
        conexclopArray = construct_array_builtin(opdatums, constraintNKeys, OIDOID);
    } else {
        conexclopArray = null_mut();
    }

    /* initialize nulls and values */
    i = 0;
    while (i as usize) < Natts_pg_constraint {
        nulls[i as usize] = false;
        values[i as usize] = 0 as Datum;
        i += 1;
    }

    conOid = GetNewOidWithIndex(conDesc, ConstraintOidIndexId, Anum_pg_constraint_oid);
    values[Anum_pg_constraint_oid as usize - 1] = ObjectIdGetDatum(conOid);
    values[Anum_pg_constraint_conname as usize - 1] = NameGetDatum(&cname);
    values[Anum_pg_constraint_connamespace as usize - 1] = ObjectIdGetDatum(constraintNamespace);
    values[Anum_pg_constraint_contype as usize - 1] = CharGetDatum(constraintType);
    values[Anum_pg_constraint_condeferrable as usize - 1] = BoolGetDatum(isDeferrable);
    values[Anum_pg_constraint_condeferred as usize - 1] = BoolGetDatum(isDeferred);
    values[Anum_pg_constraint_conenforced as usize - 1] = BoolGetDatum(isEnforced);
    values[Anum_pg_constraint_convalidated as usize - 1] = BoolGetDatum(isValidated);
    values[Anum_pg_constraint_conrelid as usize - 1] = ObjectIdGetDatum(relId);
    values[Anum_pg_constraint_contypid as usize - 1] = ObjectIdGetDatum(domainId);
    values[Anum_pg_constraint_conindid as usize - 1] = ObjectIdGetDatum(indexRelId);
    values[Anum_pg_constraint_conparentid as usize - 1] = ObjectIdGetDatum(parentConstrId);
    values[Anum_pg_constraint_confrelid as usize - 1] = ObjectIdGetDatum(foreignRelId);
    values[Anum_pg_constraint_confupdtype as usize - 1] = CharGetDatum(foreignUpdateType);
    values[Anum_pg_constraint_confdeltype as usize - 1] = CharGetDatum(foreignDeleteType);
    values[Anum_pg_constraint_confmatchtype as usize - 1] = CharGetDatum(foreignMatchType);
    values[Anum_pg_constraint_conislocal as usize - 1] = BoolGetDatum(conIsLocal);
    values[Anum_pg_constraint_coninhcount as usize - 1] = Int16GetDatum(conInhCount);
    values[Anum_pg_constraint_connoinherit as usize - 1] = BoolGetDatum(conNoInherit);
    values[Anum_pg_constraint_conperiod as usize - 1] = BoolGetDatum(conPeriod);

    if !conkeyArray.is_null() {
        values[Anum_pg_constraint_conkey as usize - 1] = PointerGetDatum(conkeyArray as *const c_void);
    } else {
        nulls[Anum_pg_constraint_conkey as usize - 1] = true;
    }

    if !confkeyArray.is_null() {
        values[Anum_pg_constraint_confkey as usize - 1] = PointerGetDatum(confkeyArray as *const c_void);
    } else {
        nulls[Anum_pg_constraint_confkey as usize - 1] = true;
    }

    if !conpfeqopArray.is_null() {
        values[Anum_pg_constraint_conpfeqop as usize - 1] = PointerGetDatum(conpfeqopArray as *const c_void);
    } else {
        nulls[Anum_pg_constraint_conpfeqop as usize - 1] = true;
    }

    if !conppeqopArray.is_null() {
        values[Anum_pg_constraint_conppeqop as usize - 1] = PointerGetDatum(conppeqopArray as *const c_void);
    } else {
        nulls[Anum_pg_constraint_conppeqop as usize - 1] = true;
    }

    if !conffeqopArray.is_null() {
        values[Anum_pg_constraint_conffeqop as usize - 1] = PointerGetDatum(conffeqopArray as *const c_void);
    } else {
        nulls[Anum_pg_constraint_conffeqop as usize - 1] = true;
    }

    if !confdelsetcolsArray.is_null() {
        values[Anum_pg_constraint_confdelsetcols as usize - 1] =
            PointerGetDatum(confdelsetcolsArray as *const c_void);
    } else {
        nulls[Anum_pg_constraint_confdelsetcols as usize - 1] = true;
    }

    if !conexclopArray.is_null() {
        values[Anum_pg_constraint_conexclop as usize - 1] = PointerGetDatum(conexclopArray as *const c_void);
    } else {
        nulls[Anum_pg_constraint_conexclop as usize - 1] = true;
    }

    if !conBin.is_null() {
        values[Anum_pg_constraint_conbin as usize - 1] = CStringGetTextDatum(conBin);
    } else {
        nulls[Anum_pg_constraint_conbin as usize - 1] = true;
    }

    tup = heap_form_tuple(RelationGetDescr(conDesc), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(conDesc, tup);

    ObjectAddressSet!(conobject, ConstraintRelationId, conOid);

    table_close(conDesc, RowExclusiveLock);

    /* Handle set of auto dependencies */
    addrs_auto = new_object_addresses();

    if OidIsValid(relId) {
        /*
         * Register auto dependency from constraint to owning relation, or to
         * specific column(s) if any are mentioned.
         */
        let mut relobject: ObjectAddress = core::mem::zeroed();

        if constraintNTotalKeys > 0 {
            i = 0;
            while i < constraintNTotalKeys {
                ObjectAddressSubSet!(
                    relobject,
                    RelationRelationId,
                    relId,
                    *constraintKey.add(i as usize) as c_int
                );
                add_exact_object_address(&relobject, addrs_auto);
                i += 1;
            }
        } else {
            ObjectAddressSet!(relobject, RelationRelationId, relId);
            add_exact_object_address(&relobject, addrs_auto);
        }
    }

    if OidIsValid(domainId) {
        /*
         * Register auto dependency from constraint to owning domain
         */
        let mut domobject: ObjectAddress = core::mem::zeroed();

        ObjectAddressSet!(domobject, TypeRelationId, domainId);
        add_exact_object_address(&domobject, addrs_auto);
    }

    record_object_address_dependencies(&conobject, addrs_auto, DEPENDENCY_AUTO);
    free_object_addresses(addrs_auto);

    /* Handle set of normal dependencies */
    addrs_normal = new_object_addresses();

    if OidIsValid(foreignRelId) {
        /*
         * Register normal dependency from constraint to foreign relation, or
         * to specific column(s) if any are mentioned.
         */
        let mut relobject: ObjectAddress = core::mem::zeroed();

        if foreignNKeys > 0 {
            i = 0;
            while i < foreignNKeys {
                ObjectAddressSubSet!(
                    relobject,
                    RelationRelationId,
                    foreignRelId,
                    *foreignKey.add(i as usize) as c_int
                );
                add_exact_object_address(&relobject, addrs_normal);
                i += 1;
            }
        } else {
            ObjectAddressSet!(relobject, RelationRelationId, foreignRelId);
            add_exact_object_address(&relobject, addrs_normal);
        }
    }

    if OidIsValid(indexRelId) && constraintType == CONSTRAINT_FOREIGN {
        /*
         * Register normal dependency on the unique index that supports a
         * foreign-key constraint.  (Note: for indexes associated with unique
         * or primary-key constraints, the dependency runs the other way, and
         * is not made here.)
         */
        let mut relobject: ObjectAddress = core::mem::zeroed();

        ObjectAddressSet!(relobject, RelationRelationId, indexRelId);
        add_exact_object_address(&relobject, addrs_normal);
    }

    if foreignNKeys > 0 {
        /*
         * Register normal dependencies on the equality operators that support
         * a foreign-key constraint.  If the PK and FK types are the same then
         * all three operators for a column are the same; otherwise they are
         * different.
         */
        let mut oprobject: ObjectAddress = core::mem::zeroed();

        oprobject.classId = OperatorRelationId;
        oprobject.objectSubId = 0;

        i = 0;
        while i < foreignNKeys {
            oprobject.objectId = *pfEqOp.add(i as usize);
            add_exact_object_address(&oprobject, addrs_normal);
            if *ppEqOp.add(i as usize) != *pfEqOp.add(i as usize) {
                oprobject.objectId = *ppEqOp.add(i as usize);
                add_exact_object_address(&oprobject, addrs_normal);
            }
            if *ffEqOp.add(i as usize) != *pfEqOp.add(i as usize) {
                oprobject.objectId = *ffEqOp.add(i as usize);
                add_exact_object_address(&oprobject, addrs_normal);
            }
            i += 1;
        }
    }

    record_object_address_dependencies(&conobject, addrs_normal, DEPENDENCY_NORMAL);
    free_object_addresses(addrs_normal);

    /*
     * We don't bother to register dependencies on the exclusion operators of
     * an exclusion constraint.  We assume they are members of the opclass
     * supporting the index, so there's an indirect dependency via that. (This
     * would be pretty dicey for cross-type operators, but exclusion operators
     * can never be cross-type.)
     */

    if !conExpr.is_null() {
        /*
         * Register dependencies from constraint to objects mentioned in CHECK
         * expression.
         */
        recordDependencyOnSingleRelExpr(
            &conobject,
            conExpr,
            relId,
            DEPENDENCY_NORMAL,
            DEPENDENCY_NORMAL,
            false,
        );
    }

    /* Post creation hook for new constraint */
    InvokeObjectPostCreateHookArg(ConstraintRelationId, conOid, 0, is_internal);

    conOid
}

/*
 * Test whether given name is currently used as a constraint name
 * for the given object (relation or domain).
 *
 * This is used to decide whether to accept a user-specified constraint name.
 * It is deliberately not the same test as ChooseConstraintName uses to decide
 * whether an auto-generated name is OK: here, we will allow it unless there
 * is an identical constraint name in use *on the same object*.
 *
 * NB: Caller should hold exclusive lock on the given object, else
 * this test can be fooled by concurrent additions.
 */
pub unsafe fn ConstraintNameIsUsed(
    conCat: ConstraintCategory,
    objId: Oid,
    conname: *const c_char,
) -> bool {
    let found: bool;
    let conDesc: Relation;
    let conscan: SysScanDesc;
    let mut skey: [ScanKeyData; 3] = core::mem::zeroed();

    conDesc = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(if conCat == CONSTRAINT_RELATION { objId } else { InvalidOid }),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(if conCat == CONSTRAINT_DOMAIN { objId } else { InvalidOid }),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(conname),
    );

    conscan = systable_beginscan(
        conDesc,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    /* There can be at most one matching row */
    found = HeapTupleIsValid(systable_getnext(conscan) as HeapTuple);

    systable_endscan(conscan);
    table_close(conDesc, AccessShareLock);

    found
}

/*
 * Does any constraint of the given name exist in the given namespace?
 *
 * This is used for code that wants to match ChooseConstraintName's rule
 * that we should avoid autogenerating duplicate constraint names within a
 * namespace.
 */
pub unsafe fn ConstraintNameExists(conname: *const c_char, namespaceid: Oid) -> bool {
    let found: bool;
    let conDesc: Relation;
    let conscan: SysScanDesc;
    let mut skey: [ScanKeyData; 2] = core::mem::zeroed();

    conDesc = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(conname),
    );

    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_connamespace,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(namespaceid),
    );

    conscan = systable_beginscan(
        conDesc,
        ConstraintNameNspIndexId,
        true,
        null_mut(),
        2,
        skey.as_mut_ptr(),
    );

    found = HeapTupleIsValid(systable_getnext(conscan) as HeapTuple);

    systable_endscan(conscan);
    table_close(conDesc, AccessShareLock);

    found
}

/*
 * Select a nonconflicting name for a new constraint.
 *
 * The objective here is to choose a name that is unique within the
 * specified namespace.  Postgres does not require this, but the SQL
 * spec does, and some apps depend on it.  Therefore we avoid choosing
 * default names that so conflict.
 *
 * Returns a palloc'd string.
 */
pub unsafe fn ChooseConstraintName(
    name1: *const c_char,
    name2: *const c_char,
    label: *const c_char,
    namespaceid: Oid,
    others: *mut List,
) -> *mut c_char {
    let mut pass: c_int = 0;
    let mut conname: *mut c_char;
    let mut modlabel: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let conDesc: Relation;
    let mut conscan: SysScanDesc;
    let mut skey: [ScanKeyData; 2] = core::mem::zeroed();
    let mut found: bool;

    conDesc = table_open(ConstraintRelationId, AccessShareLock);

    /* try the unmodified label first, unless it's empty */
    if *label != 0 {
        strlcpy(modlabel.as_mut_ptr(), label, core::mem::size_of_val(&modlabel));
    } else {
        pass += 1;
        snprintf_modlabel(modlabel.as_mut_ptr(), core::mem::size_of_val(&modlabel), label, pass);
    }

    loop {
        conname = makeObjectName(name1, name2, modlabel.as_ptr());

        found = false;

        foreach!(l, others, {
            if strcmp(lfirst(crate::current_cell!(l)) as *mut c_char, conname) == 0 {
                found = true;
                break;
            }
        });

        if !found {
            ScanKeyInit(
                &mut skey[0],
                Anum_pg_constraint_conname,
                BTEqualStrategyNumber,
                F_NAMEEQ,
                CStringGetDatum(conname),
            );

            ScanKeyInit(
                &mut skey[1],
                Anum_pg_constraint_connamespace,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(namespaceid),
            );

            conscan = systable_beginscan(
                conDesc,
                ConstraintNameNspIndexId,
                true,
                null_mut(),
                2,
                skey.as_mut_ptr(),
            );

            found = HeapTupleIsValid(systable_getnext(conscan) as HeapTuple);

            systable_endscan(conscan);
        }

        if !found {
            break;
        }

        /* found a conflict, so try a new name component */
        pfree(conname as *mut c_void);
        pass += 1;
        snprintf_modlabel(modlabel.as_mut_ptr(), core::mem::size_of_val(&modlabel), label, pass);
    }

    table_close(conDesc, AccessShareLock);

    conname
}

/*
 * AdjustNotNullInheritance
 *		Adjust inheritance status for a single not-null constraint
 *
 * If no not-null constraint is found for the column, return false.
 * Caller can create one.
 */
pub unsafe fn AdjustNotNullInheritance(
    relid: Oid,
    attnum: AttrNumber,
    new_conname: *const c_char,
    is_local: bool,
    is_no_inherit: bool,
    is_notvalid: bool,
) -> bool {
    let tup: HeapTuple;

    tup = findNotNullConstraintAttnum(relid, attnum);
    if HeapTupleIsValid(tup) {
        let pg_constraint: Relation;
        let conform: Form_pg_constraint;
        let mut changed = false;

        pg_constraint = table_open(ConstraintRelationId, RowExclusiveLock);
        conform = GETSTRUCT(tup) as Form_pg_constraint;

        /*
         * If the NO INHERIT flag we're asked for doesn't match what the
         * existing constraint has, throw an error.
         */
        if is_no_inherit != (*conform).connoinherit {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{}\"",
                    CStr_to_display(NameStr_local(&(*conform).conname)),
                    CStr_to_display(get_rel_name(relid))
                )
            );
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             * errhint("You might need to make the existing constraint inheritable using %s.",
             *         "ALTER TABLE ... ALTER CONSTRAINT ... INHERIT") */
        }

        /*
         * Throw an error if the existing constraint is NOT VALID and caller
         * wants a valid one.
         */
        if !is_notvalid && !(*conform).convalidated {
            ereport!(
                ERROR,
                errmsg!(
                    "incompatible NOT VALID constraint \"{}\" on relation \"{}\"",
                    CStr_to_display(NameStr_local(&(*conform).conname)),
                    CStr_to_display(get_rel_name(relid))
                )
            );
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             * errhint("You might need to validate it using %s.",
             *         "ALTER TABLE ... VALIDATE CONSTRAINT") */
        }

        /*
         * If, for a new constraint that is being defined locally, a name was
         * specified, then verify that the existing constraint has the same
         * name.  Otherwise throw an error.
         */
        if is_local
            && !new_conname.is_null()
            && strcmp(new_conname, NameStr_local(&(*conform).conname)) != 0
        {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot create not-null constraint \"{}\" on column \"{}\" of table \"{}\"",
                    CStr_to_display(new_conname),
                    CStr_to_display(get_attname(relid, attnum, false)),
                    CStr_to_display(get_rel_name(relid))
                )
            );
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             * errdetail("A not-null constraint named \"%s\" already exists for this column.",
             *           NameStr(conform->conname)) */
        }

        if !is_local {
            if pg_add_s16_overflow((*conform).coninhcount, 1, &mut (*conform).coninhcount) {
                ereport!(ERROR, errmsg!("too many inheritance parents"));
                /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
            }
            changed = true;
        } else if !(*conform).conislocal {
            (*conform).conislocal = true;
            changed = true;
        }

        if changed {
            CatalogTupleUpdate(pg_constraint, &mut (*tup).t_self, tup);
        }

        table_close(pg_constraint, RowExclusiveLock);

        return true;
    }

    false
}

/*
 * RelationGetNotNullConstraints
 *		Return the list of not-null constraints for the given rel
 *
 * 'include_noinh' determines whether to include NO INHERIT constraints or not.
 */
pub unsafe fn RelationGetNotNullConstraints(
    relid: Oid,
    cooked: bool,
    include_noinh: bool,
) -> *mut List {
    let mut notnulls: *mut List = null_mut();
    let constrRel: Relation;
    let mut htup: HeapTuple;
    let conscan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();

    constrRel = table_open(ConstraintRelationId, AccessShareLock);
    ScanKeyInit(
        &mut skey,
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    conscan = systable_beginscan(
        constrRel,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        1,
        &mut skey,
    );

    loop {
        htup = systable_getnext(conscan) as HeapTuple;
        if !HeapTupleIsValid(htup) {
            break;
        }
        let conForm = GETSTRUCT(htup) as Form_pg_constraint;
        let colnum: AttrNumber;

        if (*conForm).contype != CONSTRAINT_NOTNULL {
            continue;
        }
        if (*conForm).connoinherit && !include_noinh {
            continue;
        }

        colnum = extractNotNullColumn(htup);

        if cooked {
            let ck: *mut CookedConstraint;

            ck = palloc(core::mem::size_of::<CookedConstraint>()) as *mut CookedConstraint;

            (*ck).contype = CONSTR_NOTNULL;
            (*ck).conoid = (*conForm).oid;
            (*ck).name = pstrdup(NameStr_local(&(*conForm).conname));
            (*ck).attnum = colnum;
            (*ck).expr = null_mut();
            (*ck).is_enforced = true;
            (*ck).skip_validation = !(*conForm).convalidated;
            (*ck).is_local = true;
            (*ck).inhcount = 0;
            (*ck).is_no_inherit = (*conForm).connoinherit;

            notnulls = lappend(notnulls, ck as *mut c_void);
        } else {
            let constr: *mut Constraint;

            constr = makeNode!(Constraint, T_Constraint);
            (*constr).contype = CONSTR_NOTNULL;
            (*constr).conname = pstrdup(NameStr_local(&(*conForm).conname));
            (*constr).deferrable = false;
            (*constr).initdeferred = false;
            (*constr).location = -1;
            (*constr).keys = list_make1(makeString(get_attname(relid, colnum, false)) as *mut c_void);
            (*constr).is_enforced = true;
            (*constr).skip_validation = !(*conForm).convalidated;
            (*constr).initially_valid = true;
            (*constr).is_no_inherit = (*conForm).connoinherit;
            notnulls = lappend(notnulls, constr as *mut c_void);
        }
    }

    systable_endscan(conscan);
    table_close(constrRel, AccessShareLock);

    notnulls
}

/*
 * Delete a single constraint record.
 */
pub unsafe fn RemoveConstraintById(conId: Oid) {
    let conDesc: Relation;
    let tup: HeapTuple;
    let con: Form_pg_constraint;

    conDesc = table_open(ConstraintRelationId, RowExclusiveLock);

    tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conId));
    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(ERROR, "cache lookup failed for constraint {}", conId);
    }
    con = GETSTRUCT(tup) as Form_pg_constraint;

    /*
     * Special processing depending on what the constraint is for.
     */
    if OidIsValid((*con).conrelid) {
        let rel: Relation;

        /*
         * If the constraint is for a relation, open and exclusive-lock the
         * relation it's for.
         */
        rel = table_open((*con).conrelid, AccessExclusiveLock);

        /*
         * We need to update the relchecks count if it is a check constraint
         * being dropped.
         */
        if (*con).contype == CONSTRAINT_CHECK {
            let pgrel: Relation;
            let relTup: HeapTuple;
            let classForm: Form_pg_class;

            pgrel = table_open(RelationRelationId, RowExclusiveLock);
            relTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum((*con).conrelid));
            if !HeapTupleIsValid(relTup) {
                elog!(ERROR, "cache lookup failed for relation {}", (*con).conrelid);
            }
            classForm = GETSTRUCT(relTup) as Form_pg_class;

            if (*classForm).relchecks == 0 {
                /* should not happen */
                elog!(
                    ERROR,
                    "relation \"{}\" has relchecks = 0",
                    CStr_to_display(RelationGetRelationName(rel))
                );
            }
            (*classForm).relchecks -= 1;

            CatalogTupleUpdate(pgrel, &mut (*relTup).t_self, relTup);

            heap_freetuple(relTup);

            table_close(pgrel, RowExclusiveLock);
        }

        /* Keep lock on constraint's rel until end of xact */
        table_close(rel, NoLock);
    } else if OidIsValid((*con).contypid) {
        /*
         * XXX for now, do nothing special when dropping a domain constraint
         */
    } else {
        elog!(ERROR, "constraint {} is not of a known type", conId);
    }

    /* Fry the constraint itself */
    CatalogTupleDelete(conDesc, &mut (*tup).t_self);

    /* Clean up */
    ReleaseSysCache(tup);
    table_close(conDesc, RowExclusiveLock);
}

/*
 * RenameConstraintById
 *		Rename a constraint.
 */
pub unsafe fn RenameConstraintById(conId: Oid, newname: *const c_char) {
    let conDesc: Relation;
    let tuple: HeapTuple;
    let con: Form_pg_constraint;

    conDesc = table_open(ConstraintRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(CONSTROID, ObjectIdGetDatum(conId));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for constraint {}", conId);
    }
    con = GETSTRUCT(tuple) as Form_pg_constraint;

    /*
     * For user-friendliness, check whether the name is already in use.
     */
    if OidIsValid((*con).conrelid)
        && ConstraintNameIsUsed(CONSTRAINT_RELATION, (*con).conrelid, newname)
    {
        ereport!(
            ERROR,
            errmsg!(
                "constraint \"{}\" for relation \"{}\" already exists",
                CStr_to_display(newname),
                CStr_to_display(get_rel_name((*con).conrelid))
            )
        );
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }
    if OidIsValid((*con).contypid)
        && ConstraintNameIsUsed(CONSTRAINT_DOMAIN, (*con).contypid, newname)
    {
        ereport!(
            ERROR,
            errmsg!(
                "constraint \"{}\" for domain {} already exists",
                CStr_to_display(newname),
                CStr_to_display(format_type_be((*con).contypid))
            )
        );
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }

    /* OK, do the rename --- tuple is a copy, so OK to scribble on it */
    namestrcpy(&mut (*con).conname, newname);

    CatalogTupleUpdate(conDesc, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(ConstraintRelationId, conId, 0);

    heap_freetuple(tuple);
    table_close(conDesc, RowExclusiveLock);
}

/*
 * AlterConstraintNamespaces
 *		Find any constraints belonging to the specified object,
 *		and move them to the specified new namespace.
 *
 * isType indicates whether the owning object is a type or a relation.
 */
pub unsafe fn AlterConstraintNamespaces(
    ownerId: Oid,
    oldNspId: Oid,
    newNspId: Oid,
    isType: bool,
    objsMoved: *mut ObjectAddresses,
) {
    let conRel: Relation;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    conRel = table_open(ConstraintRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(if isType { InvalidOid } else { ownerId }),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(if isType { ownerId } else { InvalidOid }),
    );

    scan = systable_beginscan(
        conRel,
        ConstraintRelidTypidNameIndexId,
        true,
        null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }
        let mut conform = GETSTRUCT(tup) as Form_pg_constraint;
        let mut thisobj: ObjectAddress = core::mem::zeroed();

        ObjectAddressSet!(thisobj, ConstraintRelationId, (*conform).oid);

        if object_address_present(&thisobj, objsMoved) {
            continue;
        }

        /* Don't update if the object is already part of the namespace */
        if (*conform).connamespace == oldNspId && oldNspId != newNspId {
            tup = heap_copytuple(tup);
            conform = GETSTRUCT(tup) as Form_pg_constraint;

            (*conform).connamespace = newNspId;

            CatalogTupleUpdate(conRel, &mut (*tup).t_self, tup);

            /*
             * Note: currently, the constraint will not have its own
             * dependency on the namespace, so we don't need to do
             * changeDependencyFor().
             */
        }

        InvokeObjectPostAlterHook(ConstraintRelationId, thisobj.objectId, 0);

        add_exact_object_address(&thisobj, objsMoved);
    }

    systable_endscan(scan);

    table_close(conRel, RowExclusiveLock);
}

/*
 * ConstraintSetParentConstraint
 *		Set a partition's constraint as child of its parent constraint,
 *		or remove the linkage if parentConstrId is InvalidOid.
 */
pub unsafe fn ConstraintSetParentConstraint(
    childConstrId: Oid,
    parentConstrId: Oid,
    childTableId: Oid,
) {
    let constrRel: Relation;
    let constrForm: Form_pg_constraint;
    let tuple: HeapTuple;
    let newtup: HeapTuple;
    let mut depender: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    constrRel = table_open(ConstraintRelationId, RowExclusiveLock);
    tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(childConstrId));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for constraint {}", childConstrId);
    }
    newtup = heap_copytuple(tuple);
    constrForm = GETSTRUCT(newtup) as Form_pg_constraint;
    if OidIsValid(parentConstrId) {
        /* don't allow setting parent for a constraint that already has one */
        Assert!((*constrForm).coninhcount == 0);
        if (*constrForm).conparentid != InvalidOid {
            elog!(
                ERROR,
                "constraint {} already has a parent constraint",
                childConstrId
            );
        }

        (*constrForm).conislocal = false;
        if pg_add_s16_overflow((*constrForm).coninhcount, 1, &mut (*constrForm).coninhcount) {
            ereport!(ERROR, errmsg!("too many inheritance parents"));
            /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
        }

        (*constrForm).conparentid = parentConstrId;

        CatalogTupleUpdate(constrRel, &mut (*tuple).t_self, newtup);

        ObjectAddressSet!(depender, ConstraintRelationId, childConstrId);

        ObjectAddressSet!(referenced, ConstraintRelationId, parentConstrId);
        recordDependencyOn(&depender, &referenced, DEPENDENCY_PARTITION_PRI);

        ObjectAddressSet!(referenced, RelationRelationId, childTableId);
        recordDependencyOn(&depender, &referenced, DEPENDENCY_PARTITION_SEC);
    } else {
        (*constrForm).coninhcount -= 1;
        (*constrForm).conislocal = true;
        (*constrForm).conparentid = InvalidOid;

        /* Make sure there's no further inheritance. */
        Assert!((*constrForm).coninhcount == 0);

        CatalogTupleUpdate(constrRel, &mut (*tuple).t_self, newtup);

        deleteDependencyRecordsForClass(
            ConstraintRelationId,
            childConstrId,
            ConstraintRelationId,
            DEPENDENCY_PARTITION_PRI,
        );
        deleteDependencyRecordsForClass(
            ConstraintRelationId,
            childConstrId,
            RelationRelationId,
            DEPENDENCY_PARTITION_SEC,
        );
    }

    ReleaseSysCache(tuple);
    table_close(constrRel, RowExclusiveLock);
}

/*
 * Extract data from the pg_constraint tuple of a foreign-key constraint.
 *
 * All arguments save the first are output arguments.  All output arguments
 * other than numfks, conkey and confkey can be passed as NULL if caller
 * doesn't need them.
 */
pub unsafe fn DeconstructFkConstraintRow(
    tuple: HeapTuple,
    numfks: *mut c_int,
    conkey: *mut AttrNumber,
    confkey: *mut AttrNumber,
    pf_eq_oprs: *mut Oid,
    pp_eq_oprs: *mut Oid,
    ff_eq_oprs: *mut Oid,
    num_fk_del_set_cols: *mut c_int,
    fk_del_set_cols: *mut AttrNumber,
) {
    let mut adatum: Datum;
    let mut isNull: bool = false;
    let mut arr: *mut ArrayType;
    let numkeys: c_int;

    /*
     * We expect the arrays to be 1-D arrays of the right types; verify that.
     */
    adatum = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conkey);
    arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
    if ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != INT2OID {
        elog!(ERROR, "conkey is not a 1-D smallint array");
    }
    numkeys = *ARR_DIMS(arr).add(0);
    if numkeys <= 0 || numkeys > INDEX_MAX_KEYS as c_int {
        elog!(ERROR, "foreign key constraint cannot have {} columns", numkeys);
    }
    core::ptr::copy_nonoverlapping(
        ARR_DATA_PTR(arr) as *const int16,
        conkey as *mut int16,
        numkeys as usize,
    );
    if arr as Pointer != DatumGetPointer(adatum) {
        pfree(arr as *mut c_void); /* free de-toasted copy, if any */
    }

    adatum = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_confkey);
    arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
    if ARR_NDIM(arr) != 1
        || *ARR_DIMS(arr).add(0) != numkeys
        || ARR_HASNULL(arr)
        || ARR_ELEMTYPE(arr) != INT2OID
    {
        elog!(ERROR, "confkey is not a 1-D smallint array");
    }
    core::ptr::copy_nonoverlapping(
        ARR_DATA_PTR(arr) as *const int16,
        confkey as *mut int16,
        numkeys as usize,
    );
    if arr as Pointer != DatumGetPointer(adatum) {
        pfree(arr as *mut c_void);
    }

    if !pf_eq_oprs.is_null() {
        adatum = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conpfeqop);
        arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
        /* see TryReuseForeignKey if you change the test below */
        if ARR_NDIM(arr) != 1
            || *ARR_DIMS(arr).add(0) != numkeys
            || ARR_HASNULL(arr)
            || ARR_ELEMTYPE(arr) != OIDOID
        {
            elog!(ERROR, "conpfeqop is not a 1-D Oid array");
        }
        core::ptr::copy_nonoverlapping(
            ARR_DATA_PTR(arr) as *const Oid,
            pf_eq_oprs,
            numkeys as usize,
        );
        if arr as Pointer != DatumGetPointer(adatum) {
            pfree(arr as *mut c_void);
        }
    }

    if !pp_eq_oprs.is_null() {
        adatum = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conppeqop);
        arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
        if ARR_NDIM(arr) != 1
            || *ARR_DIMS(arr).add(0) != numkeys
            || ARR_HASNULL(arr)
            || ARR_ELEMTYPE(arr) != OIDOID
        {
            elog!(ERROR, "conppeqop is not a 1-D Oid array");
        }
        core::ptr::copy_nonoverlapping(
            ARR_DATA_PTR(arr) as *const Oid,
            pp_eq_oprs,
            numkeys as usize,
        );
        if arr as Pointer != DatumGetPointer(adatum) {
            pfree(arr as *mut c_void);
        }
    }

    if !ff_eq_oprs.is_null() {
        adatum = SysCacheGetAttrNotNull(CONSTROID, tuple, Anum_pg_constraint_conffeqop);
        arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
        if ARR_NDIM(arr) != 1
            || *ARR_DIMS(arr).add(0) != numkeys
            || ARR_HASNULL(arr)
            || ARR_ELEMTYPE(arr) != OIDOID
        {
            elog!(ERROR, "conffeqop is not a 1-D Oid array");
        }
        core::ptr::copy_nonoverlapping(
            ARR_DATA_PTR(arr) as *const Oid,
            ff_eq_oprs,
            numkeys as usize,
        );
        if arr as Pointer != DatumGetPointer(adatum) {
            pfree(arr as *mut c_void);
        }
    }

    if !fk_del_set_cols.is_null() {
        adatum = SysCacheGetAttr(
            CONSTROID,
            tuple,
            Anum_pg_constraint_confdelsetcols,
            &mut isNull,
        );
        if isNull {
            *num_fk_del_set_cols = 0;
        } else {
            let num_delete_cols: c_int;

            arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
            if ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != INT2OID {
                elog!(ERROR, "confdelsetcols is not a 1-D smallint array");
            }
            num_delete_cols = *ARR_DIMS(arr).add(0);
            core::ptr::copy_nonoverlapping(
                ARR_DATA_PTR(arr) as *const int16,
                fk_del_set_cols as *mut int16,
                num_delete_cols as usize,
            );
            if arr as Pointer != DatumGetPointer(adatum) {
                pfree(arr as *mut c_void);
            }

            *num_fk_del_set_cols = num_delete_cols;
        }
    }

    *numfks = numkeys;
}

/*
 * FindFKPeriodOpers -
 *
 * Looks up the operator oids used for the PERIOD part of a temporal foreign key.
 */
pub unsafe fn FindFKPeriodOpers(
    opclass: Oid,
    containedbyoperoid: *mut Oid,
    aggedcontainedbyoperoid: *mut Oid,
    intersectoperoid: *mut Oid,
) {
    let mut opfamily: Oid = InvalidOid;
    let mut opcintype: Oid = InvalidOid;
    let mut strat: StrategyNumber = 0;

    /* Make sure we have a range or multirange. */
    if get_opclass_opfamily_and_input_type(opclass, &mut opfamily, &mut opcintype) {
        if opcintype != ANYRANGEOID && opcintype != ANYMULTIRANGEOID {
            ereport!(
                ERROR,
                errmsg!("invalid type for PERIOD part of foreign key")
            );
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             * errdetail("Only range and multirange are supported.") */
        }
    } else {
        elog!(ERROR, "cache lookup failed for opclass {}", opclass);
    }

    /*
     * Look up the ContainedBy operator whose lhs and rhs are the opclass's
     * type.
     */
    GetOperatorFromCompareType(
        opclass,
        InvalidOid,
        COMPARE_CONTAINED_BY,
        containedbyoperoid,
        &mut strat,
    );

    /*
     * Now look up the ContainedBy operator.  Its left arg must be the type of
     * the column (or rather of the opclass).  Its right arg must match the
     * return type of the support proc.
     */
    GetOperatorFromCompareType(
        opclass,
        ANYMULTIRANGEOID,
        COMPARE_CONTAINED_BY,
        aggedcontainedbyoperoid,
        &mut strat,
    );

    match opcintype {
        x if x == ANYRANGEOID => {
            *intersectoperoid = OID_RANGE_INTERSECT_RANGE_OP;
        }
        x if x == ANYMULTIRANGEOID => {
            *intersectoperoid = OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP;
        }
        _ => {
            elog!(ERROR, "unexpected opcintype: {}", opcintype);
        }
    }
}

/* ----------------------------------------------------------------
 * Local stubs for helpers not yet ported elsewhere in src/.
 * ----------------------------------------------------------------
 */

/// TODO(pg-port): utils/adt/arrayfuncs.c DatumGetArrayTypeP (detoast wrapper)
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    null_mut()
}

/// TODO(pg-port): utils/adt/format_type.c format_type_be
unsafe fn format_type_be(type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(type_oid)
}

/*
 * ObjectAddressSet / ObjectAddressSubSet (objectaddress.h) - field initializers
 * for ObjectAddress.  Defined locally (these macros are not #[macro_export]ed).
 */
macro_rules! ObjectAddressSet {
    ($addr:expr, $classId:expr, $objectId:expr) => {{
        $addr.classId = $classId;
        $addr.objectId = $objectId;
        $addr.objectSubId = 0;
    }};
}
macro_rules! ObjectAddressSubSet {
    ($addr:expr, $classId:expr, $objectId:expr, $subId:expr) => {{
        $addr.classId = $classId;
        $addr.objectId = $objectId;
        $addr.objectSubId = $subId as i32;
    }};
}
use {ObjectAddressSet, ObjectAddressSubSet};

/// NameStr(&NameData) -> *const c_char (c.h).
unsafe fn NameStr_local(name: *const NameData) -> *const c_char {
    (*name).data.as_ptr() as *const c_char
}

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

/// strlcpy wrapper; emulates snprintf("%s%d", label, n) into modlabel.
unsafe fn snprintf_modlabel(buf: *mut c_char, size: usize, label: *const c_char, n: c_int) {
    let s = format!(
        "{}{}",
        std::ffi::CStr::from_ptr(label).to_string_lossy(),
        n
    );
    let bytes = s.as_bytes();
    let copylen = core::cmp::min(bytes.len(), size.saturating_sub(1));
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, copylen);
    *buf.add(copylen) = 0;
}

/// TODO(pg-port): common/string.c strlcpy
unsafe fn strlcpy(dst: *mut c_char, src: *const c_char, size: usize) -> usize {
    let s = std::ffi::CStr::from_ptr(src).to_bytes();
    let copylen = core::cmp::min(s.len(), size.saturating_sub(1));
    core::ptr::copy_nonoverlapping(s.as_ptr() as *const c_char, dst, copylen);
    *dst.add(copylen) = 0;
    s.len()
}

/// TODO(pg-port): utils/adt/arrayfuncs.c construct_array_builtin
unsafe fn construct_array_builtin(_elems: *mut Datum, _nelems: c_int, _elmtype: Oid) -> *mut ArrayType {
    null_mut()
}

/// TODO(pg-port): catalog/catalog.c GetNewOidWithIndex
unsafe fn GetNewOidWithIndex(_relation: Relation, _indexId: Oid, _oidcolumn: AttrNumber) -> Oid {
    InvalidOid
}

/// TODO(pg-port): catalog/indexing.c CatalogTupleInsert
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {}

/// TODO(pg-port): catalog/indexing.c CatalogTupleUpdate
unsafe fn CatalogTupleUpdate(
    _heapRel: Relation,
    _otid: *mut crate::storage::itemptr::ItemPointerData,
    _tup: HeapTuple,
) {
}

/// TODO(pg-port): catalog/indexing.c CatalogTupleDelete
unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut crate::storage::itemptr::ItemPointerData) {}

/// TODO(pg-port): commands/dbcommands.c namestrcpy
unsafe fn namestrcpy(name: *mut NameData, s: *const c_char) -> c_int {
    let src = std::ffi::CStr::from_ptr(s).to_bytes();
    let dst = (*name).data.as_mut_ptr() as *mut u8;
    let n = core::cmp::min(src.len(), core::mem::size_of::<NameData>() - 1);
    core::ptr::copy_nonoverlapping(src.as_ptr(), dst, n);
    *dst.add(n) = 0;
    0
}

/// TODO(pg-port): postgres.h NameGetDatum
unsafe fn NameGetDatum(name: *const NameData) -> Datum {
    name as Datum
}

/// TODO(pg-port): builtins.h CStringGetTextDatum
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    0 as Datum
}

/// TODO(pg-port): catalog/dependency.c new_object_addresses
unsafe fn new_object_addresses() -> *mut ObjectAddresses {
    null_mut()
}

/// TODO(pg-port): catalog/dependency.c add_exact_object_address
unsafe fn add_exact_object_address(_object: *const ObjectAddress, _addrs: *mut ObjectAddresses) {}

/// TODO(pg-port): catalog/dependency.c record_object_address_dependencies
unsafe fn record_object_address_dependencies(
    _depender: *const ObjectAddress,
    _referenced: *mut ObjectAddresses,
    _behavior: c_char,
) {
}

/// TODO(pg-port): catalog/dependency.c free_object_addresses
unsafe fn free_object_addresses(_addrs: *mut ObjectAddresses) {}

/// TODO(pg-port): catalog/dependency.c recordDependencyOnSingleRelExpr
unsafe fn recordDependencyOnSingleRelExpr(
    _depender: *const ObjectAddress,
    _expr: *mut Node,
    _relId: Oid,
    _behavior: c_char,
    _self_behavior: c_char,
    _reverse_self: bool,
) {
}

/// TODO(pg-port): catalog/pg_depend.c recordDependencyOn
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_char,
) {
}

/// TODO(pg-port): catalog/pg_depend.c deleteDependencyRecordsForClass
unsafe fn deleteDependencyRecordsForClass(
    _classId: Oid,
    _objectId: Oid,
    _refclassId: Oid,
    _deptype: c_char,
) -> c_long {
    0
}

/// TODO(pg-port): catalog/objectaccess.c InvokeObjectPostCreateHookArg
unsafe fn InvokeObjectPostCreateHookArg(
    _classId: Oid,
    _objectId: Oid,
    _objectSubId: c_int,
    _is_internal: bool,
) {
}

/// TODO(pg-port): catalog/objectaccess.c InvokeObjectPostAlterHook
unsafe fn InvokeObjectPostAlterHook(_classId: Oid, _objectId: Oid, _subId: c_int) {}

/// TODO(pg-port): catalog/namespace.c makeObjectName
unsafe fn makeObjectName(
    _name1: *const c_char,
    _name2: *const c_char,
    _label: *const c_char,
) -> *mut c_char {
    null_mut()
}

/// TODO(pg-port): utils/cache/syscache.c SearchSysCache1
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    null_mut()
}

/// TODO(pg-port): utils/cache/syscache.c SearchSysCacheCopy1
unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    null_mut()
}

/// TODO(pg-port): utils/cache/syscache.c ReleaseSysCache
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {}

/// TODO(pg-port): utils/cache/syscache.c SysCacheGetAttr
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: AttrNumber,
    isNull: *mut bool,
) -> Datum {
    *isNull = true;
    0 as Datum
}

/// TODO(pg-port): utils/cache/lsyscache.c get_attname
unsafe fn get_attname(_relid: Oid, _attnum: AttrNumber, _missing_ok: bool) -> *mut c_char {
    null_mut()
}

/// TODO(pg-port): nodes/list.c list_make1
unsafe fn list_make1(datum: *mut c_void) -> *mut List {
    lappend(null_mut(), datum)
}

/// TODO(pg-port): utils/cache/lsyscache.c get_opclass_opfamily_and_input_type
unsafe fn get_opclass_opfamily_and_input_type(
    _opclass: Oid,
    _opfamily: *mut Oid,
    _opcintype: *mut Oid,
) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // conname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_constraint, conname), 4);
        // connamespace follows the NAMEDATALEN-byte conname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_constraint, connamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_constraint>()
                >= core::mem::offset_of!(FormData_pg_constraint, conperiod)
                    + core::mem::size_of::<bool>()
        );
    }
}
