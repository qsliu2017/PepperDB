//! Translation of postgres/src/backend/storage/large_object/inv_api.c
//!
//!-------------------------------------------------------------------------
//!
//! inv_api.c
//!	  routines for manipulating inversion fs large objects. This file
//!	  contains the user-level large object application interface routines.
//!
//!
//! Note: we access pg_largeobject.data using its C struct declaration.
//! This is safe because it immediately follows pageno which is an int4 field,
//! and therefore the data field will always be 4-byte aligned, even if it
//! is in the short 1-byte-header format.  We have to detoast it since it's
//! quite likely to be in compressed or short format.  We also need to check
//! for NULLs, since initdb will mark loid and pageno but not data as NOT NULL.
//!
//! Note: many of these routines leak memory in CurrentMemoryContext, as indeed
//! does most of the backend code.  We expect that CurrentMemoryContext will
//! be a short-lived context.  Data that must persist across function calls
//! is kept either in CacheMemoryContext (the Relation structs) or in the
//! memory context given to inv_open (for LargeObjectDesc structs).
//!
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//!
//! IDENTIFICATION
//!	  src/backend/storage/large_object/inv_api.c
//!
//!-------------------------------------------------------------------------

use crate::prelude::*;

use crate::access::attnum::AttrNumber;
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::htup_details::{GETSTRUCT, HeapTuple, HeapTupleHasNulls, HeapTupleIsValid};
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple, heap_modify_tuple};
use crate::access::index::genam::{
    systable_beginscan_ordered, systable_endscan_ordered, systable_getnext_ordered, SysScanDesc,
};
use crate::access::index::indexam::{index_close, index_open};
use crate::access::common::detoast::detoast_attr;
use crate::access::sdir::{BackwardScanDirection, ForwardScanDirection};
use crate::access::stratnum::{BTEqualStrategyNumber, BTGreaterEqualStrategyNumber};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog_oids::LargeObjectRelationId;
use crate::catalog::indexing::{
    CatalogCloseIndexes, CatalogIndexState, CatalogOpenIndexes, CatalogTupleDelete,
    CatalogTupleInsertWithInfo, CatalogTupleUpdateWithInfo,
};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_largeobject::{Form_pg_largeobject, FormData_pg_largeobject};
use crate::libpq::libpq_fs::{INV_READ, INV_WRITE};
use crate::miscadmin::GetUserId;
use crate::nodes::parsenodes::{ACL_SELECT, ACL_UPDATE, DropBehavior};
use crate::storage::lockdefs::{NoLock, RowExclusiveLock};
use crate::storage::large_object::{
    LargeObjectDesc, IFS_RDLOCK, IFS_WRLOCK, LOBLKSIZE, MAX_LARGE_OBJECT_SIZE,
};
use crate::utils::adt::acl::{
    pg_largeobject_aclcheck_snapshot, AclResult, GetActiveSnapshot, LargeObjectExistsWithSnapshot,
};
use crate::utils::resowner::resowner::{
    CurrentResourceOwner, ResourceOwner, TopTransactionResourceOwner,
};
use crate::utils::rel::{Relation, RelationGetDescr};
use crate::utils::snapshot::Snapshot;
use crate::varatt::{SET_VARSIZE, VARATT_IS_EXTENDED, VARDATA, VARHDRSZ, VARSIZE};

use crate::{ereport, errmsg, elog, Assert};

// TODO(pg-port): real errmsg_internal lives in utils/elog.h (not yet #[macro_export]ed
// at the crate root).  Local shim mirroring the procsignal.rs definition: like
// errmsg!, it just produces the formatted message string for the ereport! shim.
macro_rules! errmsg_internal {
    ($($arg:tt)*) => { format!($($arg)*) };
}

// ---------------------------------------------------------------------------
// Stubs for symbols whose real home is not yet translated.
// ---------------------------------------------------------------------------

// TODO(pg-port): real LargeObjectLOidPNIndexId lives in catalog/pg_largeobject_d.h.
/* DECLARE_UNIQUE_INDEX_PKEY(pg_largeobject_loid_pn_index, 2683, LargeObjectLOidPNIndexId, ...) */
const LargeObjectLOidPNIndexId: Oid = 2683;

// TODO(pg-port): real Anum_/Natts_pg_largeobject live in catalog/pg_largeobject_d.h.
const Anum_pg_largeobject_loid: AttrNumber = 1;
const Anum_pg_largeobject_pageno: AttrNumber = 2;
const Anum_pg_largeobject_data: AttrNumber = 3;
const Natts_pg_largeobject: usize = 3;

// TODO(pg-port): real F_OIDEQ / F_INT4GE live in utils/fmgroids.h.
const F_OIDEQ: RegProcedure = 184;
const F_INT4GE: RegProcedure = 150;

// TODO(pg-port): real ERRCODE_* values live in utils/errcodes.h.
const ERRCODE_DATA_CORRUPTED: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;

// TODO(pg-port): real SEEK_SET / SEEK_CUR / SEEK_END come from <stdio.h>.
const SEEK_SET: c_int = 0;
const SEEK_CUR: c_int = 1;
const SEEK_END: c_int = 2;

// TODO(pg-port): real LargeObjectCreate lives in catalog/pg_largeobject.c.
unsafe fn LargeObjectCreate(_loid: Oid) -> Oid {
    unimplemented!()
}

// TODO(pg-port): real recordDependencyOnOwner lives in catalog/pg_shdepend.c.
unsafe fn recordDependencyOnOwner(_classId: Oid, _objectId: Oid, _owner: Oid) {
    unimplemented!()
}

// TODO(pg-port): real InvokeObjectPostCreateHook lives in catalog/objectaccess.h.
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {}

// TODO(pg-port): real CommandCounterIncrement lives in access/transam/xact.c.
unsafe fn CommandCounterIncrement() {
    unimplemented!()
}

// TODO(pg-port): real performDeletion lives in catalog/dependency.c.
unsafe fn performDeletion(_object: *const ObjectAddress, _behavior: DropBehavior, _flags: c_int) {
    unimplemented!()
}

/*
 * Offset of the variable-length `data` bytea within an on-disk
 * FormData_pg_largeobject tuple.  In C this is &(tuple->data); the ported
 * fixed struct omits the varlen `data` field, so we reach it directly --- it
 * immediately follows the int4 `pageno`, hence sits at the end of the fixed
 * part (see note at top of file).
 */
const LO_DATA_OFFSET: usize = core::mem::size_of::<FormData_pg_largeobject>();

/*
 * GUC: backwards-compatibility flag to suppress LO permission checks.
 *
 * (Defined in storage/large_object.rs as `lo_compat_privileges`.)
 */
use crate::storage::large_object::lo_compat_privileges;

/*
 * All accesses to pg_largeobject and its index make use of a single
 * Relation reference.  To guarantee that the relcache entry remains
 * in the cache, on the first reference inside a subtransaction, we
 * execute a slightly klugy maneuver to assign ownership of the
 * Relation reference to TopTransactionResourceOwner.
 */
static mut lo_heap_r: Relation = null_mut();
static mut lo_index_r: Relation = null_mut();


/*
 * Open pg_largeobject and its index, if not already done in current xact
 */
unsafe fn open_lo_relation() {
    let currentOwner: ResourceOwner;

    if !lo_heap_r.is_null() && !lo_index_r.is_null() {
        return; /* already open in current xact */
    }

    /* Arrange for the top xact to own these relation references */
    currentOwner = CurrentResourceOwner;
    CurrentResourceOwner = TopTransactionResourceOwner;

    /* Use RowExclusiveLock since we might either read or write */
    if lo_heap_r.is_null() {
        lo_heap_r = table_open(LargeObjectRelationId, RowExclusiveLock);
    }
    if lo_index_r.is_null() {
        lo_index_r = index_open(LargeObjectLOidPNIndexId, RowExclusiveLock);
    }

    CurrentResourceOwner = currentOwner;
}

/*
 * Clean up at main transaction end
 */
pub unsafe fn close_lo_relation(isCommit: bool) {
    if !lo_heap_r.is_null() || !lo_index_r.is_null() {
        /*
         * Only bother to close if committing; else abort cleanup will handle
         * it
         */
        if isCommit {
            let currentOwner: ResourceOwner;

            currentOwner = CurrentResourceOwner;
            CurrentResourceOwner = TopTransactionResourceOwner;

            if !lo_index_r.is_null() {
                index_close(lo_index_r, NoLock);
            }
            if !lo_heap_r.is_null() {
                table_close(lo_heap_r, NoLock);
            }

            CurrentResourceOwner = currentOwner;
        }
        lo_heap_r = null_mut();
        lo_index_r = null_mut();
    }
}


/*
 * Extract data field from a pg_largeobject tuple, detoasting if needed
 * and verifying that the length is sane.  Returns data pointer (a bytea *),
 * data length, and an indication of whether to pfree the data pointer.
 */
unsafe fn getdatafield(
    tuple: Form_pg_largeobject,
    pdatafield: *mut *mut bytea,
    plen: *mut c_int,
    pfreeit: *mut bool,
) {
    let mut datafield: *mut bytea;
    let len: c_int;
    let mut freeit: bool;

    datafield = (tuple as *mut c_char).add(LO_DATA_OFFSET) as *mut bytea; /* see note at top of file */
    freeit = false;
    if VARATT_IS_EXTENDED(datafield as *const c_char) {
        datafield = detoast_attr(datafield as *mut varlena) as *mut bytea;
        freeit = true;
    }
    len = VARSIZE(datafield as *const c_char) as c_int - VARHDRSZ;
    if len < 0 || len > LOBLKSIZE as c_int {
        let _ = errcode(ERRCODE_DATA_CORRUPTED);
        ereport!(
            ERROR,
            errmsg!(
                "pg_largeobject entry for OID {}, page {} has invalid data field size {}",
                (*tuple).loid,
                (*tuple).pageno,
                len
            )
        );
    }
    *pdatafield = datafield;
    *plen = len;
    *pfreeit = freeit;
}


/*
 *	inv_create -- create a new large object
 *
 *	Arguments:
 *	  lobjId - OID to use for new large object, or InvalidOid to pick one
 *
 *	Returns:
 *	  OID of new object
 *
 * If lobjId is not InvalidOid, then an error occurs if the OID is already
 * in use.
 */
pub unsafe fn inv_create(lobjId: Oid) -> Oid {
    let lobjId_new: Oid;

    /*
     * Create a new largeobject with empty data pages
     */
    lobjId_new = LargeObjectCreate(lobjId);

    /*
     * dependency on the owner of largeobject
     *
     * Note that LO dependencies are recorded using classId
     * LargeObjectRelationId for backwards-compatibility reasons.  Using
     * LargeObjectMetadataRelationId instead would simplify matters for the
     * backend, but it'd complicate pg_dump and possibly break other clients.
     */
    recordDependencyOnOwner(LargeObjectRelationId, lobjId_new, GetUserId());

    /* Post creation hook for new large object */
    InvokeObjectPostCreateHook(LargeObjectRelationId, lobjId_new, 0);

    /*
     * Advance command counter to make new tuple visible to later operations.
     */
    CommandCounterIncrement();

    lobjId_new
}

/*
 *	inv_open -- access an existing large object.
 *
 * Returns a large object descriptor, appropriately filled in.
 * The descriptor and subsidiary data are allocated in the specified
 * memory context, which must be suitably long-lived for the caller's
 * purposes.  If the returned descriptor has a snapshot associated
 * with it, the caller must ensure that it also lives long enough,
 * e.g. by calling RegisterSnapshotOnOwner
 */
pub unsafe fn inv_open(lobjId: Oid, flags: c_int, mcxt: MemoryContext) -> *mut LargeObjectDesc {
    let retval: *mut LargeObjectDesc;
    let mut snapshot: Snapshot = null_mut();
    let mut descflags: c_int = 0;

    /*
     * Historically, no difference is made between (INV_WRITE) and (INV_WRITE
     * | INV_READ), the caller being allowed to read the large object
     * descriptor in either case.
     */
    if flags & INV_WRITE != 0 {
        descflags |= IFS_WRLOCK | IFS_RDLOCK;
    }
    if flags & INV_READ != 0 {
        descflags |= IFS_RDLOCK;
    }

    if descflags == 0 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("invalid flags for opening a large object: {}", flags)
        );
    }

    /* Get snapshot.  If write is requested, use an instantaneous snapshot. */
    if descflags & IFS_WRLOCK != 0 {
        snapshot = null_mut();
    } else {
        snapshot = GetActiveSnapshot();
    }

    /* Can't use LargeObjectExists here because we need to specify snapshot */
    if !LargeObjectExistsWithSnapshot(lobjId, snapshot) {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(ERROR, errmsg!("large object {} does not exist", lobjId));
    }

    /* Apply permission checks, again specifying snapshot */
    if descflags & IFS_RDLOCK != 0 {
        if !lo_compat_privileges
            && pg_largeobject_aclcheck_snapshot(lobjId, GetUserId(), ACL_SELECT, snapshot)
                != AclResult::ACLCHECK_OK
        {
            let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            ereport!(
                ERROR,
                errmsg!("permission denied for large object {}", lobjId)
            );
        }
    }
    if descflags & IFS_WRLOCK != 0 {
        if !lo_compat_privileges
            && pg_largeobject_aclcheck_snapshot(lobjId, GetUserId(), ACL_UPDATE, snapshot)
                != AclResult::ACLCHECK_OK
        {
            let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
            ereport!(
                ERROR,
                errmsg!("permission denied for large object {}", lobjId)
            );
        }
    }

    /* OK to create a descriptor */
    retval = MemoryContextAlloc(mcxt, core::mem::size_of::<LargeObjectDesc>()) as *mut LargeObjectDesc;
    (*retval).id = lobjId;
    (*retval).offset = 0;
    (*retval).flags = descflags;

    /* caller sets if needed, not used by the functions in this file */
    (*retval).subid = InvalidSubTransactionId;

    /*
     * The snapshot (if any) is just the currently active snapshot.  The
     * caller will replace it with a longer-lived copy if needed.
     */
    (*retval).snapshot = snapshot as crate::storage::large_object::Snapshot;

    retval
}

/*
 * Closes a large object descriptor previously made by inv_open(), and
 * releases the long-term memory used by it.
 */
pub unsafe fn inv_close(obj_desc: *mut LargeObjectDesc) {
    Assert!(PointerIsValid(obj_desc));
    pfree(obj_desc as *mut c_void);
}

/*
 * Destroys an existing large object (not to be confused with a descriptor!)
 *
 * Note we expect caller to have done any required permissions check.
 */
pub unsafe fn inv_drop(lobjId: Oid) -> c_int {
    let mut object: ObjectAddress = core::mem::zeroed();

    /*
     * Delete any comments and dependencies on the large object
     */
    object.classId = LargeObjectRelationId;
    object.objectId = lobjId;
    object.objectSubId = 0;
    performDeletion(&object, DropBehavior::DROP_CASCADE, 0);

    /*
     * Advance command counter so that tuple removal will be seen by later
     * large-object operations in this transaction.
     */
    CommandCounterIncrement();

    /* For historical reasons, we always return 1 on success. */
    1
}

/*
 * Determine size of a large object
 *
 * NOTE: LOs can contain gaps, just like Unix files.  We actually return
 * the offset of the last byte + 1.
 */
unsafe fn inv_getsize(obj_desc: *mut LargeObjectDesc) -> uint64 {
    let mut lastbyte: uint64 = 0;
    let mut skey: [ScanKeyData; 1] = core::mem::zeroed();
    let sd: SysScanDesc;
    let tuple: HeapTuple;

    Assert!(PointerIsValid(obj_desc));

    open_lo_relation();

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_largeobject_loid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*obj_desc).id),
    );

    sd = systable_beginscan_ordered(
        lo_heap_r,
        lo_index_r,
        (*obj_desc).snapshot as *mut core::ffi::c_void,
        1,
        skey.as_mut_ptr(),
    );

    /*
     * Because the pg_largeobject index is on both loid and pageno, but we
     * constrain only loid, a backwards scan should visit all pages of the
     * large object in reverse pageno order.  So, it's sufficient to examine
     * the first valid tuple (== last valid page).
     */
    tuple = systable_getnext_ordered(sd, BackwardScanDirection) as *mut crate::access::htup_details::HeapTupleData;
    if HeapTupleIsValid(tuple) {
        let data: Form_pg_largeobject;
        let mut datafield: *mut bytea = null_mut();
        let mut len: c_int = 0;
        let mut pfreeit: bool = false;

        if HeapTupleHasNulls(tuple) {
            /* paranoia */
            elog!(ERROR, "null field found in pg_largeobject");
        }
        data = GETSTRUCT(tuple) as Form_pg_largeobject;
        getdatafield(data, &mut datafield, &mut len, &mut pfreeit);
        lastbyte = (*data).pageno as uint64 * LOBLKSIZE as uint64 + len as uint64;
        if pfreeit {
            pfree(datafield as *mut c_void);
        }
    }

    systable_endscan_ordered(sd);

    lastbyte
}

pub unsafe fn inv_seek(obj_desc: *mut LargeObjectDesc, offset: int64, whence: c_int) -> int64 {
    let newoffset: int64;

    Assert!(PointerIsValid(obj_desc));

    /*
     * We allow seek/tell if you have either read or write permission, so no
     * need for a permission check here.
     */

    /*
     * Note: overflow in the additions is possible, but since we will reject
     * negative results, we don't need any extra test for that.
     */
    match whence {
        SEEK_SET => {
            newoffset = offset;
        }
        SEEK_CUR => {
            newoffset = (*obj_desc).offset as int64 + offset;
        }
        SEEK_END => {
            newoffset = inv_getsize(obj_desc) as int64 + offset;
        }
        _ => {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(ERROR, errmsg!("invalid whence setting: {}", whence));
            newoffset = 0; /* keep compiler quiet */
        }
    }

    /*
     * use errmsg_internal here because we don't want to expose INT64_FORMAT
     * in translatable strings; doing better is not worth the trouble
     */
    if newoffset < 0 || newoffset > MAX_LARGE_OBJECT_SIZE {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg_internal!("invalid large object seek target: {}", newoffset)
        );
    }

    (*obj_desc).offset = newoffset as uint64;
    newoffset
}

pub unsafe fn inv_tell(obj_desc: *mut LargeObjectDesc) -> int64 {
    Assert!(PointerIsValid(obj_desc));

    /*
     * We allow seek/tell if you have either read or write permission, so no
     * need for a permission check here.
     */

    (*obj_desc).offset as int64
}

pub unsafe fn inv_read(obj_desc: *mut LargeObjectDesc, buf: *mut c_char, nbytes: c_int) -> c_int {
    let mut nread: c_int = 0;
    let mut n: int64;
    let mut off: int64;
    let mut len: c_int = 0;
    let pageno: int32 = ((*obj_desc).offset / LOBLKSIZE as uint64) as int32;
    let mut pageoff: uint64;
    let mut skey: [ScanKeyData; 2] = core::mem::zeroed();
    let sd: SysScanDesc;
    let mut tuple: HeapTuple;

    Assert!(PointerIsValid(obj_desc));
    Assert!(!buf.is_null());

    if (*obj_desc).flags & IFS_RDLOCK == 0 {
        let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
        ereport!(
            ERROR,
            errmsg!("permission denied for large object {}", (*obj_desc).id)
        );
    }

    if nbytes <= 0 {
        return 0;
    }

    open_lo_relation();

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_largeobject_loid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*obj_desc).id),
    );

    ScanKeyInit(
        &mut skey[1],
        Anum_pg_largeobject_pageno,
        BTGreaterEqualStrategyNumber,
        F_INT4GE,
        Int32GetDatum(pageno),
    );

    sd = systable_beginscan_ordered(
        lo_heap_r,
        lo_index_r,
        (*obj_desc).snapshot as *mut core::ffi::c_void,
        2,
        skey.as_mut_ptr(),
    );

    loop {
        tuple = systable_getnext_ordered(sd, ForwardScanDirection) as *mut crate::access::htup_details::HeapTupleData;
        if tuple.is_null() {
            break;
        }

        let data: Form_pg_largeobject;
        let mut datafield: *mut bytea = null_mut();
        let mut pfreeit: bool = false;

        if HeapTupleHasNulls(tuple) {
            /* paranoia */
            elog!(ERROR, "null field found in pg_largeobject");
        }
        data = GETSTRUCT(tuple) as Form_pg_largeobject;

        /*
         * We expect the indexscan will deliver pages in order.  However,
         * there may be missing pages if the LO contains unwritten "holes". We
         * want missing sections to read out as zeroes.
         */
        pageoff = (*data).pageno as uint64 * LOBLKSIZE as uint64;
        if pageoff > (*obj_desc).offset {
            n = (pageoff - (*obj_desc).offset) as int64;
            n = if n <= (nbytes - nread) as int64 {
                n
            } else {
                (nbytes - nread) as int64
            };
            MemSet(buf.add(nread as usize) as *mut c_void, 0, n as Size);
            nread += n as c_int;
            (*obj_desc).offset += n as uint64;
        }

        if nread < nbytes {
            Assert!((*obj_desc).offset >= pageoff);
            off = ((*obj_desc).offset - pageoff) as int64;
            Assert!(off >= 0 && off < LOBLKSIZE as int64);

            getdatafield(data, &mut datafield, &mut len, &mut pfreeit);
            if len as int64 > off {
                n = len as int64 - off;
                n = if n <= (nbytes - nread) as int64 {
                    n
                } else {
                    (nbytes - nread) as int64
                };
                std::ptr::copy_nonoverlapping(
                    VARDATA(datafield as *const c_char).add(off as usize),
                    buf.add(nread as usize),
                    n as usize,
                );
                nread += n as c_int;
                (*obj_desc).offset += n as uint64;
            }
            if pfreeit {
                pfree(datafield as *mut c_void);
            }
        }

        if nread >= nbytes {
            break;
        }
    }

    systable_endscan_ordered(sd);

    nread
}

pub unsafe fn inv_write(obj_desc: *mut LargeObjectDesc, buf: *const c_char, nbytes: c_int) -> c_int {
    let mut nwritten: c_int = 0;
    let mut n: c_int;
    let mut off: c_int;
    let mut len: c_int = 0;
    let mut pageno: int32 = ((*obj_desc).offset / LOBLKSIZE as uint64) as int32;
    let mut skey: [ScanKeyData; 2] = core::mem::zeroed();
    let sd: SysScanDesc;
    let mut oldtuple: HeapTuple;
    let mut olddata: Form_pg_largeobject;
    let mut neednextpage: bool;
    let mut datafield: *mut bytea = null_mut();
    let mut pfreeit: bool = false;
    /*
     * union { bytea hdr; char data[LOBLKSIZE + VARHDRSZ]; int32 align_it; }
     * workbuf -- the int32 align_it ensures 4-byte alignment, so use a fixed
     * array of int32 large enough to hold the LO data chunk plus header.
     */
    let mut workbuf: [int32; (LOBLKSIZE + VARHDRSZ as usize) / 4 + 1] =
        [0; (LOBLKSIZE + VARHDRSZ as usize) / 4 + 1];
    let workbuf_ptr = workbuf.as_mut_ptr() as *mut c_char;
    let workb: *mut c_char = VARDATA(workbuf_ptr);
    let mut newtup: HeapTuple;
    let mut values: [Datum; Natts_pg_largeobject] = [0; Natts_pg_largeobject];
    let mut nulls: [bool; Natts_pg_largeobject] = [false; Natts_pg_largeobject];
    let mut replace: [bool; Natts_pg_largeobject] = [false; Natts_pg_largeobject];
    let indstate: CatalogIndexState;

    Assert!(PointerIsValid(obj_desc));
    Assert!(!buf.is_null());

    /* enforce writability because snapshot is probably wrong otherwise */
    if (*obj_desc).flags & IFS_WRLOCK == 0 {
        let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
        ereport!(
            ERROR,
            errmsg!("permission denied for large object {}", (*obj_desc).id)
        );
    }

    if nbytes <= 0 {
        return 0;
    }

    /* this addition can't overflow because nbytes is only int32 */
    if (nbytes as int64 + (*obj_desc).offset as int64) > MAX_LARGE_OBJECT_SIZE {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("invalid large object write request size: {}", nbytes)
        );
    }

    open_lo_relation();

    indstate = CatalogOpenIndexes(lo_heap_r);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_largeobject_loid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*obj_desc).id),
    );

    ScanKeyInit(
        &mut skey[1],
        Anum_pg_largeobject_pageno,
        BTGreaterEqualStrategyNumber,
        F_INT4GE,
        Int32GetDatum(pageno),
    );

    sd = systable_beginscan_ordered(
        lo_heap_r,
        lo_index_r,
        (*obj_desc).snapshot as *mut core::ffi::c_void,
        2,
        skey.as_mut_ptr(),
    );

    oldtuple = null_mut();
    olddata = null_mut();
    neednextpage = true;

    while nwritten < nbytes {
        /*
         * If possible, get next pre-existing page of the LO.  We expect the
         * indexscan will deliver these in order --- but there may be holes.
         */
        if neednextpage {
            oldtuple = systable_getnext_ordered(sd, ForwardScanDirection) as *mut crate::access::htup_details::HeapTupleData;
            if !oldtuple.is_null() {
                if HeapTupleHasNulls(oldtuple) {
                    /* paranoia */
                    elog!(ERROR, "null field found in pg_largeobject");
                }
                olddata = GETSTRUCT(oldtuple) as Form_pg_largeobject;
                Assert!((*olddata).pageno >= pageno);
            }
            neednextpage = false;
        }

        /*
         * If we have a pre-existing page, see if it is the page we want to
         * write, or a later one.
         */
        if !olddata.is_null() && (*olddata).pageno == pageno {
            /*
             * Update an existing page with fresh data.
             *
             * First, load old data into workbuf
             */
            getdatafield(olddata, &mut datafield, &mut len, &mut pfreeit);
            std::ptr::copy_nonoverlapping(
                VARDATA(datafield as *const c_char),
                workb,
                len as usize,
            );
            if pfreeit {
                pfree(datafield as *mut c_void);
            }

            /*
             * Fill any hole
             */
            off = ((*obj_desc).offset % LOBLKSIZE as uint64) as c_int;
            if off > len {
                MemSet(
                    workb.add(len as usize) as *mut c_void,
                    0,
                    (off - len) as Size,
                );
            }

            /*
             * Insert appropriate portion of new data
             */
            n = LOBLKSIZE as c_int - off;
            n = if n <= (nbytes - nwritten) { n } else { nbytes - nwritten };
            std::ptr::copy_nonoverlapping(
                buf.add(nwritten as usize),
                workb.add(off as usize),
                n as usize,
            );
            nwritten += n;
            (*obj_desc).offset += n as uint64;
            off += n;
            /* compute valid length of new page */
            len = if len >= off { len } else { off };
            SET_VARSIZE(workbuf_ptr, len + VARHDRSZ);

            /*
             * Form and insert updated tuple
             */
            values = [0; Natts_pg_largeobject];
            nulls = [false; Natts_pg_largeobject];
            replace = [false; Natts_pg_largeobject];
            values[Anum_pg_largeobject_data as usize - 1] = PointerGetDatum(workbuf_ptr as *const c_void);
            replace[Anum_pg_largeobject_data as usize - 1] = true;
            newtup = heap_modify_tuple(
                oldtuple,
                RelationGetDescr(lo_heap_r),
                values.as_ptr(),
                nulls.as_ptr(),
                replace.as_ptr(),
            );
            CatalogTupleUpdateWithInfo(lo_heap_r, &mut (*newtup).t_self, newtup, indstate);
            heap_freetuple(newtup);

            /*
             * We're done with this old page.
             */
            oldtuple = null_mut();
            olddata = null_mut();
            neednextpage = true;
        } else {
            /*
             * Write a brand new page.
             *
             * First, fill any hole
             */
            off = ((*obj_desc).offset % LOBLKSIZE as uint64) as c_int;
            if off > 0 {
                MemSet(workb as *mut c_void, 0, off as Size);
            }

            /*
             * Insert appropriate portion of new data
             */
            n = LOBLKSIZE as c_int - off;
            n = if n <= (nbytes - nwritten) { n } else { nbytes - nwritten };
            std::ptr::copy_nonoverlapping(
                buf.add(nwritten as usize),
                workb.add(off as usize),
                n as usize,
            );
            nwritten += n;
            (*obj_desc).offset += n as uint64;
            /* compute valid length of new page */
            len = off + n;
            SET_VARSIZE(workbuf_ptr, len + VARHDRSZ);

            /*
             * Form and insert updated tuple
             */
            values = [0; Natts_pg_largeobject];
            nulls = [false; Natts_pg_largeobject];
            values[Anum_pg_largeobject_loid as usize - 1] = ObjectIdGetDatum((*obj_desc).id);
            values[Anum_pg_largeobject_pageno as usize - 1] = Int32GetDatum(pageno);
            values[Anum_pg_largeobject_data as usize - 1] = PointerGetDatum(workbuf_ptr as *const c_void);
            newtup = heap_form_tuple((*lo_heap_r).rd_att, values.as_ptr(), nulls.as_ptr());
            CatalogTupleInsertWithInfo(lo_heap_r, newtup, indstate);
            heap_freetuple(newtup);
        }
        pageno += 1;
    }

    systable_endscan_ordered(sd);

    CatalogCloseIndexes(indstate);

    /*
     * Advance command counter so that my tuple updates will be seen by later
     * large-object operations in this transaction.
     */
    CommandCounterIncrement();

    nwritten
}

pub unsafe fn inv_truncate(obj_desc: *mut LargeObjectDesc, len: int64) {
    let pageno: int32 = (len / LOBLKSIZE as int64) as int32;
    let off: int32;
    let mut skey: [ScanKeyData; 2] = core::mem::zeroed();
    let sd: SysScanDesc;
    let mut oldtuple: HeapTuple;
    let mut olddata: Form_pg_largeobject;
    /*
     * union { bytea hdr; char data[LOBLKSIZE + VARHDRSZ]; int32 align_it; }
     * workbuf -- see inv_write for the alignment rationale.
     */
    let mut workbuf: [int32; (LOBLKSIZE + VARHDRSZ as usize) / 4 + 1] =
        [0; (LOBLKSIZE + VARHDRSZ as usize) / 4 + 1];
    let workbuf_ptr = workbuf.as_mut_ptr() as *mut c_char;
    let workb: *mut c_char = VARDATA(workbuf_ptr);
    let newtup: HeapTuple;
    let mut values: [Datum; Natts_pg_largeobject] = [0; Natts_pg_largeobject];
    let mut nulls: [bool; Natts_pg_largeobject] = [false; Natts_pg_largeobject];
    let mut replace: [bool; Natts_pg_largeobject] = [false; Natts_pg_largeobject];
    let indstate: CatalogIndexState;

    Assert!(PointerIsValid(obj_desc));

    /* enforce writability because snapshot is probably wrong otherwise */
    if (*obj_desc).flags & IFS_WRLOCK == 0 {
        let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
        ereport!(
            ERROR,
            errmsg!("permission denied for large object {}", (*obj_desc).id)
        );
    }

    /*
     * use errmsg_internal here because we don't want to expose INT64_FORMAT
     * in translatable strings; doing better is not worth the trouble
     */
    if len < 0 || len > MAX_LARGE_OBJECT_SIZE {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg_internal!("invalid large object truncation target: {}", len)
        );
    }

    open_lo_relation();

    indstate = CatalogOpenIndexes(lo_heap_r);

    /*
     * Set up to find all pages with desired loid and pageno >= target
     */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_largeobject_loid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*obj_desc).id),
    );

    ScanKeyInit(
        &mut skey[1],
        Anum_pg_largeobject_pageno,
        BTGreaterEqualStrategyNumber,
        F_INT4GE,
        Int32GetDatum(pageno),
    );

    sd = systable_beginscan_ordered(
        lo_heap_r,
        lo_index_r,
        (*obj_desc).snapshot as *mut core::ffi::c_void,
        2,
        skey.as_mut_ptr(),
    );

    /*
     * If possible, get the page the truncation point is in. The truncation
     * point may be beyond the end of the LO or in a hole.
     */
    olddata = null_mut();
    oldtuple = systable_getnext_ordered(sd, ForwardScanDirection) as *mut crate::access::htup_details::HeapTupleData;
    if !oldtuple.is_null() {
        if HeapTupleHasNulls(oldtuple) {
            /* paranoia */
            elog!(ERROR, "null field found in pg_largeobject");
        }
        olddata = GETSTRUCT(oldtuple) as Form_pg_largeobject;
        Assert!((*olddata).pageno >= pageno);
    }

    /*
     * If we found the page of the truncation point we need to truncate the
     * data in it.  Otherwise if we're in a hole, we need to create a page to
     * mark the end of data.
     */
    if !olddata.is_null() && (*olddata).pageno == pageno {
        /* First, load old data into workbuf */
        let mut datafield: *mut bytea = null_mut();
        let mut pagelen: c_int = 0;
        let mut pfreeit: bool = false;

        getdatafield(olddata, &mut datafield, &mut pagelen, &mut pfreeit);
        std::ptr::copy_nonoverlapping(
            VARDATA(datafield as *const c_char),
            workb,
            pagelen as usize,
        );
        if pfreeit {
            pfree(datafield as *mut c_void);
        }

        /*
         * Fill any hole
         */
        off = (len % LOBLKSIZE as int64) as int32;
        if off > pagelen {
            MemSet(
                workb.add(pagelen as usize) as *mut c_void,
                0,
                (off - pagelen) as Size,
            );
        }

        /* compute length of new page */
        SET_VARSIZE(workbuf_ptr, off + VARHDRSZ);

        /*
         * Form and insert updated tuple
         */
        values = [0; Natts_pg_largeobject];
        nulls = [false; Natts_pg_largeobject];
        replace = [false; Natts_pg_largeobject];
        values[Anum_pg_largeobject_data as usize - 1] = PointerGetDatum(workbuf_ptr as *const c_void);
        replace[Anum_pg_largeobject_data as usize - 1] = true;
        newtup = heap_modify_tuple(
            oldtuple,
            RelationGetDescr(lo_heap_r),
            values.as_ptr(),
            nulls.as_ptr(),
            replace.as_ptr(),
        );
        CatalogTupleUpdateWithInfo(lo_heap_r, &mut (*newtup).t_self, newtup, indstate);
        heap_freetuple(newtup);
    } else {
        /*
         * If the first page we found was after the truncation point, we're in
         * a hole that we'll fill, but we need to delete the later page
         * because the loop below won't visit it again.
         */
        if !olddata.is_null() {
            Assert!((*olddata).pageno > pageno);
            CatalogTupleDelete(lo_heap_r, &mut (*oldtuple).t_self);
        }

        /*
         * Write a brand new page.
         *
         * Fill the hole up to the truncation point
         */
        off = (len % LOBLKSIZE as int64) as int32;
        if off > 0 {
            MemSet(workb as *mut c_void, 0, off as Size);
        }

        /* compute length of new page */
        SET_VARSIZE(workbuf_ptr, off + VARHDRSZ);

        /*
         * Form and insert new tuple
         */
        values = [0; Natts_pg_largeobject];
        nulls = [false; Natts_pg_largeobject];
        values[Anum_pg_largeobject_loid as usize - 1] = ObjectIdGetDatum((*obj_desc).id);
        values[Anum_pg_largeobject_pageno as usize - 1] = Int32GetDatum(pageno);
        values[Anum_pg_largeobject_data as usize - 1] = PointerGetDatum(workbuf_ptr as *const c_void);
        newtup = heap_form_tuple((*lo_heap_r).rd_att, values.as_ptr(), nulls.as_ptr());
        CatalogTupleInsertWithInfo(lo_heap_r, newtup, indstate);
        heap_freetuple(newtup);
    }

    /*
     * Delete any pages after the truncation point.  If the initial search
     * didn't find a page, then of course there's nothing more to do.
     */
    if !olddata.is_null() {
        loop {
            oldtuple = systable_getnext_ordered(sd, ForwardScanDirection) as *mut crate::access::htup_details::HeapTupleData;
            if oldtuple.is_null() {
                break;
            }
            CatalogTupleDelete(lo_heap_r, &mut (*oldtuple).t_self);
        }
    }

    systable_endscan_ordered(sd);

    CatalogCloseIndexes(indstate);

    /*
     * Advance command counter so that tuple updates will be seen by later
     * large-object operations in this transaction.
     */
    CommandCounterIncrement();
}
