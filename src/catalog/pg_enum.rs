//! Translation of postgres/src/include/catalog/pg_enum.h
//!
//! The `FormData_pg_enum` struct: the fixed-layout part of a pg_enum catalog
//! row, defining the "enum" system catalog (pg_enum) which records the label
//! values and sort positions of every enumerated-type value.
//!
//! The C header has NO `#ifdef CATALOG_VARLEN` section, so every declared
//! column is part of the fixed struct - including the trailing NameData
//! enumlabel, which is a fixed NAMEDATALEN-byte field rather than a varlen
//! attribute.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use crate::c::{float4, NameData, Name, Size, NameStr};
use crate::postgres_ext::Oid;
use crate::access::htup_details::{HeapTuple, HeapTupleData, GETSTRUCT};
use crate::storage::itemptr::ItemPointerData;
use core::ffi::{c_int, c_char, c_void, c_long};
use core::ptr;

use crate::{ereport, errmsg, Assert};
use crate::utils::elog::{ERROR, NOTICE};
use crate::nodes::pg_list::{List, ListCell, lfirst, list_length};
use crate::{foreach, current_cell};

// ---------------------------------------------------------------------------
// Local type/constant placeholders for headers / subsystems not yet wired.
// Mirrors the convention used in sibling catalog/pg_shdepend.rs.
// ---------------------------------------------------------------------------

pub type Datum = usize;
pub type Relation = *mut c_void;
pub type TupleDesc = *mut c_void;
pub type SysScanDesc = *mut c_void;
pub type SnapshotData = c_void;
pub type CatalogIndexState = *mut c_void;
pub type MemoryContext = *mut c_void;
pub type HTAB = c_void;

const InvalidOid: Oid = 0;

#[inline]
fn OidIsValid(o: Oid) -> bool {
    o != InvalidOid
}

// catalog/catalog_oids - pg_enum relation + index OIDs.
// TODO(pg-port): import from catalog OID tables once exported.
const EnumRelationId: Oid = 3501;
const TypeRelationId: Oid = 1247;
const EnumOidIndexId: Oid = 3502;
const EnumTypIdLabelIndexId: Oid = 3503;

// storage/lock.h - lock modes.
const RowExclusiveLock: c_int = 3;
const ExclusiveLock: c_int = 7;

// access/stratnum.h / utils/fmgroids.h.
const BTEqualStrategyNumber: c_int = 3;
const F_OIDEQ: Oid = 184;

// utils/syscache.h - SysCacheIdentifier ENUMTYPOIDNAME.
const ENUMTYPOIDNAME: c_int = 0;

// utils/errcodes.h.
const ERRCODE_INVALID_NAME: c_int = 0;
const ERRCODE_DUPLICATE_OBJECT: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

// catalog/pg_enum_d.h - attribute numbers / column count.
const Anum_pg_enum_oid: c_int = 1;
const Anum_pg_enum_enumtypid: c_int = 2;
const Anum_pg_enum_enumsortorder: c_int = 3;
const Anum_pg_enum_enumlabel: c_int = 4;
const Natts_pg_enum: usize = 4;

// pg_config_manual.h.
const NAMEDATALEN: usize = 64;

// access/htup.h - multi-insert batching threshold.
const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

// utils/hsearch.h - HASHCTL flags / actions.
const HASH_ELEM: c_int = 0x0008;
const HASH_BLOBS: c_int = 0x0020;
const HASH_CONTEXT: c_int = 0x0400;
const HASH_FIND: c_int = 0;
const HASH_ENTER: c_int = 1;

// utils/memutils.h - top-of-transaction memory context.
// TODO(pg-port): import crate::utils::mmgr::mcxt::TopTransactionContext.
extern "C" {
    static mut TopTransactionContext: MemoryContext;
}

#[repr(C)]
struct HASHCTL {
    keysize: Size,
    entrysize: Size,
    hcxt: MemoryContext,
}

#[repr(C)]
struct HASH_SEQ_STATUS {
    _private: [u8; 16],
}

/* executor/tuptable.h - minimal mirror of the fields used here. */
#[repr(C)]
struct TupleTableSlot {
    tts_values: *mut Datum,
    tts_isnull: *mut bool,
    tts_tupleDescriptor: *mut TupleDescData,
}
#[repr(C)]
struct TupleDescData {
    natts: c_int,
}

#[repr(C)]
struct ScanKeyData {
    _private: [u8; 64],
}

/* utils/catcache.h - CatCList / CatCTup minimal mirror. */
#[repr(C)]
struct CatCList {
    n_members: c_int,
    members: *mut *mut CatCTup,
}
#[repr(C)]
struct CatCTup {
    tuple: HeapTupleData,
}

// executor/tuptable.h slot ops marker; only its address is used here.
#[repr(C)]
struct TupleTableSlotOps {
    _private: [u8; 0],
}
static TTSOpsHeapTuple: TupleTableSlotOps = TupleTableSlotOps { _private: [] };

/*
 * FormData_pg_enum - a pg_enum row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_enum {
    /* oid */
    pub oid: Oid,
    /* OID of owning enum type */
    pub enumtypid: Oid,
    /* sort position of this enum value */
    pub enumsortorder: float4,
    /* text representation of enum value */
    pub enumlabel: NameData,
}

/*
 * Form_pg_enum corresponds to a pointer to a tuple with the format of the
 * pg_enum relation.
 */
pub type Form_pg_enum = *mut FormData_pg_enum;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * pg_enum.h exposes no #define constants to client code.
 * ----------------------------------------------------------------
 */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // enumtypid sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_enum, enumtypid), 4);
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_enum>()
                >= core::mem::offset_of!(FormData_pg_enum, enumlabel)
                    + core::mem::size_of::<NameData>()
        );
    }
}

/*
 * sort_order_cmp - qsort comparator for pg_enum tuples by enumsortorder.
 */
unsafe fn sort_order_cmp(p1: *const c_void, p2: *const c_void) -> c_int {
    let v1: HeapTuple = *(p1 as *const HeapTuple);
    let v2: HeapTuple = *(p2 as *const HeapTuple);
    let en1: Form_pg_enum = GETSTRUCT(v1) as Form_pg_enum;
    let en2: Form_pg_enum = GETSTRUCT(v2) as Form_pg_enum;

    if (*en1).enumsortorder < (*en2).enumsortorder {
        -1
    } else if (*en1).enumsortorder > (*en2).enumsortorder {
        1
    } else {
        0
    }
}

/* Potentially set by pg_upgrade_support functions */
static mut binary_upgrade_next_pg_enum_oid: Oid = InvalidOid;

/*
 * We keep two transaction-lifespan hash tables, one containing the OIDs
 * of enum types made in the current transaction, and one containing the
 * OIDs of enum values created during the current transaction by
 * AddEnumLabel (but only if their enum type is not in the first hash).
 */
static mut uncommitted_enum_types: *mut HTAB = ptr::null_mut();
static mut uncommitted_enum_values: *mut HTAB = ptr::null_mut();

/*
 * EnumValuesCreate
 *		Create an entry in pg_enum for each of the supplied enum values.
 *
 * vals is a list of String values.
 */
pub unsafe fn EnumValuesCreate(enumTypeOid: Oid, vals: *mut List) {
    let pg_enum: Relation;
    let oids: *mut Oid;
    let mut elemno: c_int;
    let num_elems: c_int;
    let lc: *mut ListCell;
    let mut slotCount: c_int = 0;
    let nslots: c_int;
    let indstate: CatalogIndexState;
    let slot: *mut *mut TupleTableSlot;

    /*
     * Remember the type OID as being made in the current transaction, but not
     * if we're in a subtransaction.
     */
    if GetCurrentTransactionNestLevel() == 1 {
        if uncommitted_enum_types.is_null() {
            init_uncommitted_enum_types();
        }
        let mut k = enumTypeOid;
        hash_search(uncommitted_enum_types, &mut k as *mut Oid as *mut c_void,
                    HASH_ENTER, ptr::null_mut());
    }

    num_elems = list_length(vals);

    /*
     * We do not bother to check the list of values for duplicates --- if you
     * have any, you'll get a less-than-friendly unique-index violation.
     */

    pg_enum = table_open(EnumRelationId, RowExclusiveLock);

    /*
     * Allocate OIDs for the enum's members.
     */
    oids = palloc((num_elems as usize) * core::mem::size_of::<Oid>()) as *mut Oid;

    elemno = 0;
    while elemno < num_elems {
        /*
         * We assign even-numbered OIDs to all the new enum labels.  This
         * tells the comparison functions the OIDs are in the correct sort
         * order and can be compared directly.
         */
        let mut new_oid: Oid;

        loop {
            new_oid = GetNewOidWithIndex(pg_enum, EnumOidIndexId, Anum_pg_enum_oid);
            if new_oid & 1 == 0 {
                break;
            }
        }
        *oids.add(elemno as usize) = new_oid;
        elemno += 1;
    }

    /* sort them, just in case OID counter wrapped from high to low */
    qsort(oids as *mut c_void, num_elems as usize, core::mem::size_of::<Oid>(), oid_cmp);

    /* and make the entries */
    indstate = CatalogOpenIndexes(pg_enum);

    /* allocate the slots to use and initialize them */
    nslots = Min(num_elems,
                 (MAX_CATALOG_MULTI_INSERT_BYTES / core::mem::size_of::<FormData_pg_enum>()) as c_int);
    slot = palloc(core::mem::size_of::<*mut TupleTableSlot>() * nslots as usize) as *mut *mut TupleTableSlot;
    {
        let mut i: c_int = 0;
        while i < nslots {
            *slot.add(i as usize) = MakeSingleTupleTableSlot(RelationGetDescr(pg_enum),
                                                             &TTSOpsHeapTuple as *const _ as *const c_void);
            i += 1;
        }
    }

    elemno = 0;
    foreach!(lc, vals, {
        let lab: *mut c_char = strVal(lfirst(current_cell!(lc)));
        let enumlabel: Name = palloc0(NAMEDATALEN) as Name;

        /*
         * labels are stored in a name field, for easier syscache lookup, so
         * check the length to make sure it's within range.
         */
        if strlen(lab) > (NAMEDATALEN - 1) {
            ereport!(ERROR, errmsg!("invalid enum label \"{}\"",
                std::ffi::CStr::from_ptr(lab).to_string_lossy()));
            /* C also: errcode(ERRCODE_INVALID_NAME),
               errdetail("Labels must be %d bytes or less.", NAMEDATALEN - 1) */
        }

        let s = *slot.add(slotCount as usize);
        ExecClearTuple(s as *mut c_void);

        memset((*s).tts_isnull as *mut c_void, 0,
               ((*(*s).tts_tupleDescriptor).natts as usize) * core::mem::size_of::<bool>());

        *(*s).tts_values.add((Anum_pg_enum_oid - 1) as usize) = ObjectIdGetDatum(*oids.add(elemno as usize));
        *(*s).tts_values.add((Anum_pg_enum_enumtypid - 1) as usize) = ObjectIdGetDatum(enumTypeOid);
        *(*s).tts_values.add((Anum_pg_enum_enumsortorder - 1) as usize) = Float4GetDatum((elemno + 1) as float4);

        namestrcpy(enumlabel, lab);
        *(*s).tts_values.add((Anum_pg_enum_enumlabel - 1) as usize) = NameGetDatum(enumlabel as *const NameData);

        ExecStoreVirtualTuple(s as *mut c_void);
        slotCount += 1;

        /* if slots are full, insert a batch of tuples */
        if slotCount == nslots {
            CatalogTuplesMultiInsertWithInfo(pg_enum, slot, slotCount, indstate);
            slotCount = 0;
        }

        elemno += 1;
    });

    /* Insert any tuples left in the buffer */
    if slotCount > 0 {
        CatalogTuplesMultiInsertWithInfo(pg_enum, slot, slotCount, indstate);
    }

    /* clean up */
    pfree(oids as *mut c_void);
    {
        let mut i: c_int = 0;
        while i < nslots {
            ExecDropSingleTupleTableSlot(*slot.add(i as usize) as *mut c_void);
            i += 1;
        }
    }
    CatalogCloseIndexes(indstate);
    table_close(pg_enum, RowExclusiveLock);
}

/*
 * EnumValuesDelete
 *		Remove all the pg_enum entries for the specified enum type.
 */
pub unsafe fn EnumValuesDelete(enumTypeOid: Oid) {
    let pg_enum: Relation;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    pg_enum = table_open(EnumRelationId, RowExclusiveLock);

    ScanKeyInit(&mut key[0],
                Anum_pg_enum_enumtypid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(enumTypeOid));

    scan = systable_beginscan(pg_enum, EnumTypIdLabelIndexId, true,
                              ptr::null_mut(), 1, key.as_mut_ptr());

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        CatalogTupleDelete(pg_enum, &mut (*tup).t_self as *mut ItemPointerData);
    }

    systable_endscan(scan);

    table_close(pg_enum, RowExclusiveLock);
}

/*
 * Initialize the uncommitted enum types table for this transaction.
 */
unsafe fn init_uncommitted_enum_types() {
    let mut hash_ctl: HASHCTL = core::mem::zeroed();

    hash_ctl.keysize = core::mem::size_of::<Oid>();
    hash_ctl.entrysize = core::mem::size_of::<Oid>();
    hash_ctl.hcxt = TopTransactionContext;
    uncommitted_enum_types = hash_create(c"Uncommitted enum types".as_ptr(),
                                         32,
                                         &mut hash_ctl,
                                         HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Initialize the uncommitted enum values table for this transaction.
 */
unsafe fn init_uncommitted_enum_values() {
    let mut hash_ctl: HASHCTL = core::mem::zeroed();

    hash_ctl.keysize = core::mem::size_of::<Oid>();
    hash_ctl.entrysize = core::mem::size_of::<Oid>();
    hash_ctl.hcxt = TopTransactionContext;
    uncommitted_enum_values = hash_create(c"Uncommitted enum values".as_ptr(),
                                          32,
                                          &mut hash_ctl,
                                          HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * AddEnumLabel
 *		Add a new label to the enum set. By default it goes at
 *		the end, but the user can choose to place it before or
 *		after any existing set member.
 */
pub unsafe fn AddEnumLabel(
    enumTypeOid: Oid,
    newVal: *const c_char,
    neighbor: *const c_char,
    newValIsAfter: bool,
    skipIfExists: bool,
) {
    let pg_enum: Relation;
    let mut newOid: Oid;
    let mut values: [Datum; Natts_pg_enum] = core::mem::zeroed();
    let mut nulls: [bool; Natts_pg_enum] = core::mem::zeroed();
    let mut enumlabel: NameData = core::mem::zeroed();
    let mut enum_tup: HeapTuple;
    let newelemorder: float4;
    let mut existing: *mut HeapTuple;
    let mut list: *mut CatCList;
    let mut nelems: c_int;
    let mut i: c_int;

    /* check length of new label is ok */
    if strlen(newVal) > (NAMEDATALEN - 1) {
        ereport!(ERROR, errmsg!("invalid enum label \"{}\"",
            std::ffi::CStr::from_ptr(newVal).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_NAME),
           errdetail("Labels must be %d bytes or less.", NAMEDATALEN - 1) */
    }

    /*
     * Acquire a lock on the enum type, which we won't release until commit.
     */
    LockDatabaseObject(TypeRelationId, enumTypeOid, 0, ExclusiveLock);

    /*
     * Check if label is already in use.
     */
    enum_tup = SearchSysCache2(ENUMTYPOIDNAME,
                               ObjectIdGetDatum(enumTypeOid),
                               CStringGetDatum(newVal));
    if HeapTupleIsValid(enum_tup) {
        ReleaseSysCache(enum_tup);
        if skipIfExists {
            ereport!(NOTICE, errmsg!("enum label \"{}\" already exists, skipping",
                std::ffi::CStr::from_ptr(newVal).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
            return;
        } else {
            ereport!(ERROR, errmsg!("enum label \"{}\" already exists",
                std::ffi::CStr::from_ptr(newVal).to_string_lossy()));
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }
    }

    pg_enum = table_open(EnumRelationId, RowExclusiveLock);

    /* If we have to renumber the existing members, we restart from here */
    'restart: loop {
        /* Get the list of existing members of the enum */
        list = SearchSysCacheList1(ENUMTYPOIDNAME, ObjectIdGetDatum(enumTypeOid));
        nelems = (*list).n_members;

        /* Sort the existing members by enumsortorder */
        existing = palloc((nelems as usize) * core::mem::size_of::<HeapTuple>()) as *mut HeapTuple;
        i = 0;
        while i < nelems {
            *existing.add(i as usize) =
                &mut (**(*list).members.add(i as usize)).tuple as *mut HeapTupleData as HeapTuple;
            i += 1;
        }

        qsort(existing as *mut c_void, nelems as usize,
              core::mem::size_of::<HeapTuple>(), sort_order_cmp_raw);

        if neighbor.is_null() {
            /*
             * Put the new label at the end of the list. No change to existing
             * tuples is required.
             */
            if nelems > 0 {
                let en: Form_pg_enum = GETSTRUCT(*existing.add((nelems - 1) as usize)) as Form_pg_enum;
                newelemorder = (*en).enumsortorder + 1.0;
            } else {
                newelemorder = 1.0;
            }
        } else {
            /* BEFORE or AFTER was specified */
            let mut nbr_index: c_int;
            let other_nbr_index: c_int;
            let nbr_en: Form_pg_enum;
            let other_nbr_en: Form_pg_enum;

            /* Locate the neighbor element */
            nbr_index = 0;
            while nbr_index < nelems {
                let en: Form_pg_enum = GETSTRUCT(*existing.add(nbr_index as usize)) as Form_pg_enum;
                if strcmp(NameStr(&(*en).enumlabel), neighbor) == 0 {
                    break;
                }
                nbr_index += 1;
            }
            if nbr_index >= nelems {
                ereport!(ERROR, errmsg!("\"{}\" is not an existing enum label",
                    std::ffi::CStr::from_ptr(neighbor).to_string_lossy()));
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            }
            nbr_en = GETSTRUCT(*existing.add(nbr_index as usize)) as Form_pg_enum;

            /*
             * Attempt to assign an appropriate enumsortorder value.
             */
            if newValIsAfter {
                other_nbr_index = nbr_index + 1;
            } else {
                other_nbr_index = nbr_index - 1;
            }

            if other_nbr_index < 0 {
                newelemorder = (*nbr_en).enumsortorder - 1.0;
            } else if other_nbr_index >= nelems {
                newelemorder = (*nbr_en).enumsortorder + 1.0;
            } else {
                /*
                 * The midpoint value computed here has to be rounded to float4
                 * precision.
                 */
                let midpoint: float4;

                other_nbr_en = GETSTRUCT(*existing.add(other_nbr_index as usize)) as Form_pg_enum;
                midpoint = ((*nbr_en).enumsortorder + (*other_nbr_en).enumsortorder) / 2.0;

                if midpoint == (*nbr_en).enumsortorder || midpoint == (*other_nbr_en).enumsortorder {
                    RenumberEnumType(pg_enum, existing, nelems);
                    /* Clean up and start over */
                    pfree(existing as *mut c_void);
                    ReleaseCatCacheList(list);
                    continue 'restart;
                }

                newelemorder = midpoint;
            }
        }

        /* Get a new OID for the new label */
        if IsBinaryUpgrade {
            if !OidIsValid(binary_upgrade_next_pg_enum_oid) {
                ereport!(ERROR, errmsg!("pg_enum OID value not set when in binary upgrade mode"));
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            }

            /*
             * Use binary-upgrade override for pg_enum.oid, if supplied.
             */
            if !neighbor.is_null() {
                ereport!(ERROR, errmsg!("ALTER TYPE ADD BEFORE/AFTER is incompatible with binary upgrade"));
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            }

            newOid = binary_upgrade_next_pg_enum_oid;
            binary_upgrade_next_pg_enum_oid = InvalidOid;
        } else {
            /*
             * Normal case: we need to allocate a new Oid for the value.
             */
            loop {
                let mut sorts_ok: bool;

                /* Get a new OID (different from all existing pg_enum tuples) */
                newOid = GetNewOidWithIndex(pg_enum, EnumOidIndexId, Anum_pg_enum_oid);

                /*
                 * Detect whether it sorts correctly relative to existing
                 * even-numbered labels of the enum.
                 */
                sorts_ok = true;
                i = 0;
                while i < nelems {
                    let exists_tup: HeapTuple = *existing.add(i as usize);
                    let exists_en: Form_pg_enum = GETSTRUCT(exists_tup) as Form_pg_enum;
                    let exists_oid: Oid = (*exists_en).oid;

                    if exists_oid & 1 != 0 {
                        i += 1;
                        continue; /* ignore odd Oids */
                    }

                    if (*exists_en).enumsortorder < newelemorder {
                        /* should sort before */
                        if exists_oid >= newOid {
                            sorts_ok = false;
                            break;
                        }
                    } else {
                        /* should sort after */
                        if exists_oid <= newOid {
                            sorts_ok = false;
                            break;
                        }
                    }
                    i += 1;
                }

                if sorts_ok {
                    /* If it's even and sorts OK, we're done. */
                    if (newOid & 1) == 0 {
                        break;
                    }

                    /*
                     * If it's odd, and sorts OK, loop back to get another OID and
                     * try again.
                     */
                } else {
                    /*
                     * If it's odd, and does not sort correctly, we're done.
                     */
                    if newOid & 1 != 0 {
                        break;
                    }

                    /*
                     * If it's even, and does not sort correctly, loop back to get
                     * another OID and try again.
                     */
                }
            }
        }

        /* Done with info about existing members */
        pfree(existing as *mut c_void);
        ReleaseCatCacheList(list);

        /* Create the new pg_enum entry */
        memset(nulls.as_mut_ptr() as *mut c_void, 0, core::mem::size_of_val(&nulls));
        values[(Anum_pg_enum_oid - 1) as usize] = ObjectIdGetDatum(newOid);
        values[(Anum_pg_enum_enumtypid - 1) as usize] = ObjectIdGetDatum(enumTypeOid);
        values[(Anum_pg_enum_enumsortorder - 1) as usize] = Float4GetDatum(newelemorder);
        namestrcpy(&mut enumlabel as *mut NameData, newVal);
        values[(Anum_pg_enum_enumlabel - 1) as usize] = NameGetDatum(&enumlabel as *const NameData);
        enum_tup = heap_form_tuple(RelationGetDescr(pg_enum), values.as_mut_ptr(), nulls.as_mut_ptr());
        CatalogTupleInsert(pg_enum, enum_tup);
        heap_freetuple(enum_tup);

        table_close(pg_enum, RowExclusiveLock);

        break;
    }

    /*
     * If the enum type itself is uncommitted, we need not enter the new enum
     * value into uncommitted_enum_values.
     */
    if GetCurrentTransactionNestLevel() == 1 && EnumTypeUncommitted(enumTypeOid) {
        return;
    }

    /* Set up the uncommitted values table if not already done in this tx */
    if uncommitted_enum_values.is_null() {
        init_uncommitted_enum_values();
    }

    /* Add the new value to the table */
    hash_search(uncommitted_enum_values, &mut newOid as *mut Oid as *mut c_void,
                HASH_ENTER, ptr::null_mut());
}

/*
 * RenameEnumLabel
 *		Rename a label in an enum set.
 */
pub unsafe fn RenameEnumLabel(enumTypeOid: Oid, oldVal: *const c_char, newVal: *const c_char) {
    let pg_enum: Relation;
    let mut enum_tup: HeapTuple;
    let mut en: Form_pg_enum;
    let list: *mut CatCList;
    let nelems: c_int;
    let mut old_tup: HeapTuple;
    let mut found_new: bool;
    let mut i: c_int;

    /* check length of new label is ok */
    if strlen(newVal) > (NAMEDATALEN - 1) {
        ereport!(ERROR, errmsg!("invalid enum label \"{}\"",
            std::ffi::CStr::from_ptr(newVal).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_NAME),
           errdetail("Labels must be %d bytes or less.", NAMEDATALEN - 1) */
    }

    /*
     * Acquire a lock on the enum type, which we won't release until commit.
     */
    LockDatabaseObject(TypeRelationId, enumTypeOid, 0, ExclusiveLock);

    pg_enum = table_open(EnumRelationId, RowExclusiveLock);

    /* Get the list of existing members of the enum */
    list = SearchSysCacheList1(ENUMTYPOIDNAME, ObjectIdGetDatum(enumTypeOid));
    nelems = (*list).n_members;

    /*
     * Locate the element to rename and check if the new label is already in
     * use.
     */
    old_tup = ptr::null_mut();
    found_new = false;
    i = 0;
    while i < nelems {
        enum_tup = &mut (**(*list).members.add(i as usize)).tuple as *mut HeapTupleData as HeapTuple;
        en = GETSTRUCT(enum_tup) as Form_pg_enum;
        if strcmp(NameStr(&(*en).enumlabel), oldVal) == 0 {
            old_tup = enum_tup;
        }
        if strcmp(NameStr(&(*en).enumlabel), newVal) == 0 {
            found_new = true;
        }
        i += 1;
    }
    if old_tup.is_null() {
        ereport!(ERROR, errmsg!("\"{}\" is not an existing enum label",
            std::ffi::CStr::from_ptr(oldVal).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }
    if found_new {
        ereport!(ERROR, errmsg!("enum label \"{}\" already exists",
            std::ffi::CStr::from_ptr(newVal).to_string_lossy()));
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }

    /* OK, make a writable copy of old tuple */
    enum_tup = heap_copytuple(old_tup);
    en = GETSTRUCT(enum_tup) as Form_pg_enum;

    ReleaseCatCacheList(list);

    /* Update the pg_enum entry */
    namestrcpy(&mut (*en).enumlabel as *mut NameData, newVal);
    CatalogTupleUpdate(pg_enum, &mut (*enum_tup).t_self as *mut ItemPointerData, enum_tup);
    heap_freetuple(enum_tup);

    table_close(pg_enum, RowExclusiveLock);
}

/*
 * Test if the given type OID is in the table of uncommitted enum types.
 */
unsafe fn EnumTypeUncommitted(typ_id: Oid) -> bool {
    let mut found: bool = false;

    /* If we've made no uncommitted types table, it's not in the table */
    if uncommitted_enum_types.is_null() {
        return false;
    }

    /* Else, is it in the table? */
    let mut k = typ_id;
    hash_search(uncommitted_enum_types, &mut k as *mut Oid as *mut c_void,
                HASH_FIND, &mut found);
    found
}

/*
 * Test if the given enum value is in the table of uncommitted enum values.
 */
pub unsafe fn EnumUncommitted(enum_id: Oid) -> bool {
    let mut found: bool = false;

    /* If we've made no uncommitted values table, it's not in the table */
    if uncommitted_enum_values.is_null() {
        return false;
    }

    /* Else, is it in the table? */
    let mut k = enum_id;
    hash_search(uncommitted_enum_values, &mut k as *mut Oid as *mut c_void,
                HASH_FIND, &mut found);
    found
}

/*
 * Clean up enum stuff after end of top-level transaction.
 */
pub unsafe fn AtEOXact_Enum() {
    /*
     * Reset the uncommitted tables, as all our tuples are now committed.
     */
    uncommitted_enum_types = ptr::null_mut();
    uncommitted_enum_values = ptr::null_mut();
}

/*
 * RenumberEnumType
 *		Renumber existing enum elements to have sort positions 1..n.
 */
unsafe fn RenumberEnumType(pg_enum: Relation, existing: *mut HeapTuple, nelems: c_int) {
    let mut i: c_int;

    /*
     * We should only need to increase existing elements' enumsortorders,
     * never decrease them.  Therefore, work from the end backwards.
     */
    i = nelems - 1;
    while i >= 0 {
        let newtup: HeapTuple;
        let en: Form_pg_enum;
        let newsortorder: float4;

        newtup = heap_copytuple(*existing.add(i as usize));
        en = GETSTRUCT(newtup) as Form_pg_enum;

        newsortorder = (i + 1) as float4;
        if (*en).enumsortorder != newsortorder {
            (*en).enumsortorder = newsortorder;

            CatalogTupleUpdate(pg_enum, &mut (*newtup).t_self as *mut ItemPointerData, newtup);
        }

        heap_freetuple(newtup);
        i -= 1;
    }

    /* Make the updates visible */
    CommandCounterIncrement();
}

/*
 * Raw-pointer qsort comparator wrapper for the typed sort_order_cmp above.
 */
unsafe fn sort_order_cmp_raw(p1: *const c_void, p2: *const c_void) -> c_int {
    sort_order_cmp(p1, p2)
}

pub unsafe fn EstimateUncommittedEnumsSpace() -> Size {
    let mut entries: usize = 0;

    if !uncommitted_enum_types.is_null() {
        entries += hash_get_num_entries(uncommitted_enum_types) as usize;
    }
    if !uncommitted_enum_values.is_null() {
        entries += hash_get_num_entries(uncommitted_enum_values) as usize;
    }

    /* Add two for the terminators. */
    core::mem::size_of::<Oid>() * (entries + 2)
}

pub unsafe fn SerializeUncommittedEnums(space: *mut c_void, size: Size) {
    let mut serialized: *mut Oid = space as *mut Oid;

    /*
     * Make sure the hash tables haven't changed in size since the caller
     * reserved the space.
     */
    Assert!(size == EstimateUncommittedEnumsSpace());

    /* Write out all the OIDs from the types hash table, if there is one. */
    if !uncommitted_enum_types.is_null() {
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
        let mut value: *mut Oid;

        hash_seq_init(&mut status, uncommitted_enum_types);
        loop {
            value = hash_seq_search(&mut status) as *mut Oid;
            if value.is_null() {
                break;
            }
            *serialized = *value;
            serialized = serialized.add(1);
        }
    }

    /* Write out the terminator. */
    *serialized = InvalidOid;
    serialized = serialized.add(1);

    /* Write out all the OIDs from the values hash table, if there is one. */
    if !uncommitted_enum_values.is_null() {
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
        let mut value: *mut Oid;

        hash_seq_init(&mut status, uncommitted_enum_values);
        loop {
            value = hash_seq_search(&mut status) as *mut Oid;
            if value.is_null() {
                break;
            }
            *serialized = *value;
            serialized = serialized.add(1);
        }
    }

    /* Write out the terminator. */
    *serialized = InvalidOid;
    serialized = serialized.add(1);

    /*
     * Make sure the amount of space we actually used matches what was
     * estimated.
     */
    Assert!(serialized as *const u8 == (space as *const u8).add(size));
}

pub unsafe fn RestoreUncommittedEnums(space: *mut c_void) {
    let mut serialized: *mut Oid = space as *mut Oid;

    Assert!(uncommitted_enum_types.is_null());
    Assert!(uncommitted_enum_values.is_null());

    /*
     * If either list is empty then don't even bother to create that hash
     * table.
     */
    if OidIsValid(*serialized) {
        /* Read all the types into a new hash table. */
        init_uncommitted_enum_types();
        loop {
            hash_search(uncommitted_enum_types, serialized as *mut c_void, HASH_ENTER, ptr::null_mut());
            serialized = serialized.add(1);
            if !OidIsValid(*serialized) {
                break;
            }
        }
    }
    serialized = serialized.add(1);
    if OidIsValid(*serialized) {
        /* Read all the values into a new hash table. */
        init_uncommitted_enum_values();
        loop {
            hash_search(uncommitted_enum_values, serialized as *mut c_void, HASH_ENTER, ptr::null_mut());
            serialized = serialized.add(1);
            if !OidIsValid(*serialized) {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TODO(pg-port): thin stubs for not-yet-wired dependencies.  Prefer importing
// the real fns once their homes stabilize across the crate.
// ---------------------------------------------------------------------------

#[inline]
fn Min(a: c_int, b: c_int) -> c_int {
    if a < b { a } else { b }
}

unsafe fn palloc(_size: usize) -> *mut c_void { unimplemented!() }
unsafe fn palloc0(_size: usize) -> *mut c_void { unimplemented!() }
unsafe fn pfree(_p: *mut c_void) { unimplemented!() }
unsafe fn memset(_s: *mut c_void, _c: c_int, _n: usize) -> *mut c_void { unimplemented!() }
unsafe fn strlen(_s: *const c_char) -> usize { unimplemented!() }
unsafe fn strcmp(_a: *const c_char, _b: *const c_char) -> c_int { unimplemented!() }
unsafe fn qsort(_base: *mut c_void, _nmemb: usize, _size: usize,
                _cmp: unsafe fn(*const c_void, *const c_void) -> c_int) { unimplemented!() }
unsafe fn strVal(_v: *mut c_void) -> *mut c_char { unimplemented!() }

unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation { unimplemented!() }
unsafe fn table_close(_relation: Relation, _lockmode: c_int) { unimplemented!() }
unsafe fn GetNewOidWithIndex(_rel: Relation, _indexId: Oid, _oidcolumn: c_int) -> Oid { unimplemented!() }
unsafe fn CatalogOpenIndexes(_rel: Relation) -> CatalogIndexState { unimplemented!() }
unsafe fn CatalogCloseIndexes(_indstate: CatalogIndexState) { unimplemented!() }
unsafe fn CatalogTuplesMultiInsertWithInfo(_rel: Relation, _slot: *mut *mut TupleTableSlot,
                                           _ntuples: c_int, _indstate: CatalogIndexState) { unimplemented!() }
unsafe fn CatalogTupleInsert(_rel: Relation, _tup: HeapTuple) { unimplemented!() }
unsafe fn CatalogTupleUpdate(_rel: Relation, _otid: *mut ItemPointerData, _tup: HeapTuple) { unimplemented!() }
unsafe fn CatalogTupleDelete(_rel: Relation, _tid: *mut ItemPointerData) { unimplemented!() }
unsafe fn RelationGetDescr(_rel: Relation) -> TupleDesc { unimplemented!() }
unsafe fn MakeSingleTupleTableSlot(_desc: TupleDesc, _ops: *const c_void) -> *mut TupleTableSlot { unimplemented!() }
unsafe fn ExecClearTuple(_slot: *mut c_void) { unimplemented!() }
unsafe fn ExecStoreVirtualTuple(_slot: *mut c_void) { unimplemented!() }
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut c_void) { unimplemented!() }
unsafe fn ScanKeyInit(_entry: *mut ScanKeyData, _attno: c_int, _strategy: c_int,
                      _procedure: Oid, _argument: Datum) { unimplemented!() }
unsafe fn systable_beginscan(_rel: Relation, _indexId: Oid, _indexOK: bool,
                             _snapshot: *mut SnapshotData, _nkeys: c_int,
                             _key: *mut ScanKeyData) -> SysScanDesc { unimplemented!() }
unsafe fn systable_getnext(_scan: SysScanDesc) -> HeapTuple { unimplemented!() }
unsafe fn systable_endscan(_scan: SysScanDesc) { unimplemented!() }
unsafe fn heap_form_tuple(_desc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) -> HeapTuple { unimplemented!() }
unsafe fn heap_copytuple(_tup: HeapTuple) -> HeapTuple { unimplemented!() }
unsafe fn heap_freetuple(_tup: HeapTuple) { unimplemented!() }
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool { !tuple.is_null() }
unsafe fn namestrcpy(_name: Name, _src: *const c_char) -> c_int { unimplemented!() }

unsafe fn LockDatabaseObject(_classid: Oid, _objid: Oid, _objsubid: u16, _lockmode: c_int) { unimplemented!() }
unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple { unimplemented!() }
unsafe fn ReleaseSysCache(_tuple: HeapTuple) { unimplemented!() }
unsafe fn SearchSysCacheList1(_cacheId: c_int, _key1: Datum) -> *mut CatCList { unimplemented!() }
unsafe fn ReleaseCatCacheList(_list: *mut CatCList) { unimplemented!() }

unsafe fn GetCurrentTransactionNestLevel() -> c_int { unimplemented!() }
unsafe fn CommandCounterIncrement() { unimplemented!() }

unsafe fn hash_create(_tabname: *const c_char, _nelem: c_long,
                      _info: *mut HASHCTL, _flags: c_int) -> *mut HTAB { unimplemented!() }
unsafe fn hash_search(_hashp: *mut HTAB, _keyPtr: *mut c_void, _action: c_int,
                      _foundPtr: *mut bool) -> *mut c_void { unimplemented!() }
unsafe fn hash_seq_init(_status: *mut HASH_SEQ_STATUS, _hashp: *mut HTAB) { unimplemented!() }
unsafe fn hash_seq_search(_status: *mut HASH_SEQ_STATUS) -> *mut c_void { unimplemented!() }
unsafe fn hash_get_num_entries(_hashp: *mut HTAB) -> c_long { unimplemented!() }

fn ObjectIdGetDatum(o: Oid) -> Datum { o as Datum }
fn Float4GetDatum(v: float4) -> Datum { v.to_bits() as Datum }
unsafe fn NameGetDatum(name: *const NameData) -> Datum { name as Datum }
unsafe fn CStringGetDatum(s: *const c_char) -> Datum { s as Datum }
unsafe fn oid_cmp(_p1: *const c_void, _p2: *const c_void) -> c_int { unimplemented!() }

// miscadmin.h - binary upgrade mode flag.
const IsBinaryUpgrade: bool = false;
