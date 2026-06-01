#![allow(unreachable_patterns)] // exhaustive C switches over partial Rust enums
/*-------------------------------------------------------------------------
 *
 * dependency.rs
 *	  Routines to support inter-object dependencies.
 *
 * Source: postgres/src/backend/catalog/dependency.c
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
use crate::prelude::*;
use crate::postgres_ext::Oid;

// Node infrastructure
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{
    List, ListCell, list_length, list_nth, lcons, list_delete_first,
    lfirst, lfirst_oid, lfirst_int,
};
use crate::{IsA, castNode, foreach, current_cell, list_make1};

// ObjectAddress (the canonical definition lives in objectaccess)
use crate::catalog::objectaccess::ObjectAddress;

// Parse / primitive node structs referenced by the expression walker
use crate::nodes::parsenodes::{
    DropBehavior, DropBehavior::*, RangeTblEntry, RangeTblFunction,
    Query, RTEKind::*, SortGroupClause, WindowClause, CTECycleClause,
    SetOperationStmt, TableSampleClause,
};
use crate::nodes::nodes::{CmdType::*};
use crate::nodes::primnodes::{
    Var, Const, Param, FuncExpr, OpExpr, DistinctExpr, NullIfExpr,
    ScalarArrayOpExpr, Aggref, WindowFunc, SubscriptingRef, SubPlan,
    FieldSelect, FieldStore, RelabelType, CoerceViaIO, ArrayCoerceExpr,
    ConvertRowtypeExpr, CollateExpr, RowExpr, RowCompareExpr, CoerceToDomain,
    NextValueExpr, OnConflictExpr, TableFunc, TargetEntry,
};

// Catalog relation OIDs
use crate::catalog::catalog_oids::*;
// Form_pg_depend
use crate::catalog::pg_depend::Form_pg_depend;

// pg_depend column numbers (catalog/pg_depend.h)
// TODO(pg-port): replace with generated pg_depend_d.h constants.
const Anum_pg_depend_classid: AttrNumber = 1;
const Anum_pg_depend_objid: AttrNumber = 2;
const Anum_pg_depend_objsubid: AttrNumber = 3;
const Anum_pg_depend_refclassid: AttrNumber = 4;
const Anum_pg_depend_refobjid: AttrNumber = 5;
const Anum_pg_depend_refobjsubid: AttrNumber = 6;

// pg_init_privs column numbers (catalog/pg_init_privs.h)
// TODO(pg-port): replace with generated pg_init_privs_d.h constants.
const Anum_pg_init_privs_objoid: AttrNumber = 1;
const Anum_pg_init_privs_classoid: AttrNumber = 2;
const Anum_pg_init_privs_objsubid: AttrNumber = 3;

// DependencyType / PERFORM_DELETION_* (catalog/dependency.h).
// dependency.rs is the home for these; defined locally per the C header.
// TODO(pg-port): replace with generated dependency_d.h constants.
pub type DependencyType = c_char;
pub const DEPENDENCY_NORMAL: DependencyType = b'n' as DependencyType;
pub const DEPENDENCY_AUTO: DependencyType = b'a' as DependencyType;
pub const DEPENDENCY_INTERNAL: DependencyType = b'i' as DependencyType;
pub const DEPENDENCY_PARTITION_PRI: DependencyType = b'P' as DependencyType;
pub const DEPENDENCY_PARTITION_SEC: DependencyType = b'S' as DependencyType;
pub const DEPENDENCY_EXTENSION: DependencyType = b'e' as DependencyType;
pub const DEPENDENCY_AUTO_EXTENSION: DependencyType = b'x' as DependencyType;

pub const PERFORM_DELETION_INTERNAL: c_int = 0x0001;
pub const PERFORM_DELETION_CONCURRENTLY: c_int = 0x0002;
pub const PERFORM_DELETION_QUIETLY: c_int = 0x0004;
pub const PERFORM_DELETION_SKIP_ORIGINAL: c_int = 0x0008;
pub const PERFORM_DELETION_SKIP_EXTENSIONS: c_int = 0x0010;
pub const PERFORM_DELETION_CONCURRENT_LOCK: c_int = 0x0020;

// access / scan / tuple
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::relscan::{ScanKeyData, SysScanDescData};
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::common::scankey::ScanKeyInit;
use crate::access::index::genam::{
    SysScanDesc, systable_beginscan, systable_getnext, systable_endscan,
    systable_recheck_tuple,
};
use crate::catalog::objectaddress_impl::{table_open, table_close};
use crate::utils::rel::{Relation, RelationData};
use crate::storage::lockdefs::{
    AccessExclusiveLock, AccessShareLock, RowExclusiveLock, ShareUpdateExclusiveLock,
};
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};

// fmgr OIDs used in ScanKeyInit (utils/fmgroids.h)
// TODO(pg-port): replace with generated fmgroids.h constants.
const F_OIDEQ: RegProcedure = 184;
const F_INT4EQ: RegProcedure = 65;

// stringinfo
use crate::lib::stringinfo::{
    StringInfoData, initStringInfo, appendStringInfoChar,
};
use crate::appendStringInfo; // #[macro_export] macro lives at crate root

// type OIDs referenced by the Const walker
use crate::catalog::pg_type_d::{
    REGPROCOID, REGPROCEDUREOID, REGOPEROID, REGOPERATOROID, REGCLASSOID,
    REGTYPEOID, REGCOLLATIONOID, REGCONFIGOID, REGDICTIONARYOID, REGNAMESPACEOID,
    REGROLEOID, RECORDOID,
};
use crate::catalog::pg_known_oids::DEFAULT_COLLATION_OID;
use crate::catalog::pg_class::{RELKIND_RELATION, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX, RELKIND_SEQUENCE};

// Datum helpers
use crate::postgres::{
    Datum, ObjectIdGetDatum, Int32GetDatum, DatumGetObjectId,
};

use core::ffi::CStr;

// errmsg_internal() is just errmsg() without translation; alias for the port.
macro_rules! errmsg_internal {
    ($fmt:literal $(, $arg:expr)*) => { errmsg!($fmt $(, $arg)*) };
}

/* ----------------------------------------------------------------
 * Deletion processing requires additional state for each ObjectAddress that
 * it's planning to delete.  For simplicity and code-sharing we make the
 * ObjectAddresses code support arrays with or without this extra state.
 * ---------------------------------------------------------------- */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAddressExtra {
    pub flags: c_int,            /* bitmask, see bit definitions below */
    pub dependee: ObjectAddress, /* object whose deletion forced this one */
}

/* ObjectAddressExtra flag bits */
const DEPFLAG_ORIGINAL: c_int = 0x0001; /* an original deletion target */
const DEPFLAG_NORMAL: c_int = 0x0002;   /* reached via normal dependency */
const DEPFLAG_AUTO: c_int = 0x0004;     /* reached via auto dependency */
const DEPFLAG_INTERNAL: c_int = 0x0008; /* reached via internal dependency */
const DEPFLAG_PARTITION: c_int = 0x0010; /* reached via partition dependency */
const DEPFLAG_EXTENSION: c_int = 0x0020; /* reached via extension dependency */
const DEPFLAG_REVERSE: c_int = 0x0040;  /* reverse internal/extension link */
const DEPFLAG_IS_PART: c_int = 0x0080;  /* has a partition dependency */
const DEPFLAG_SUBOBJECT: c_int = 0x0100; /* subobject of another deletable object */

/* expansible list of ObjectAddresses */
#[repr(C)]
pub struct ObjectAddresses {
    pub refs: *mut ObjectAddress, /* => palloc'd array */
    pub extras: *mut ObjectAddressExtra, /* => palloc'd array, or NULL if not used */
    pub numrefs: c_int,           /* current number of references */
    pub maxrefs: c_int,           /* current size of palloc'd array(s) */
}

/* threaded list of ObjectAddresses, for recursion detection */
#[repr(C)]
pub struct ObjectAddressStack {
    pub object: *const ObjectAddress, /* object being visited */
    pub flags: c_int,                 /* its current flag bits */
    pub next: *mut ObjectAddressStack, /* next outer stack level */
}

/* temporary storage in findDependentObjects */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAddressAndFlags {
    pub obj: ObjectAddress, /* object to be deleted --- MUST BE FIRST */
    pub subflags: c_int,    /* flags to pass down when recursing to obj */
}

/* for find_expr_references_walker */
#[repr(C)]
pub struct find_expr_references_context {
    pub addrs: *mut ObjectAddresses, /* addresses being accumulated */
    pub rtables: *mut List,          /* list of rangetables to resolve Vars */
}

// ----------------------------------------------------------------
//   TODO(pg-port): dependencies declared in other .c files
// ----------------------------------------------------------------

/// TODO(pg-port): catalog/catalog.c IsPinnedObject
unsafe fn IsPinnedObject(_classId: Oid, _objectId: Oid) -> bool {
    false
}

/// TODO(pg-port): catalog/objectaddress.c getObjectDescription
unsafe fn getObjectDescription(_object: *const ObjectAddress, _missing_ok: bool) -> *mut c_char {
    null_mut()
}

/// TODO(pg-port): catalog/objectaddress.c get_object_catcache_oid
unsafe fn get_object_catcache_oid(_class_id: Oid) -> c_int {
    -1
}
/// TODO(pg-port): catalog/objectaddress.c get_object_attnum_oid
unsafe fn get_object_attnum_oid(_class_id: Oid) -> AttrNumber {
    0
}
/// TODO(pg-port): catalog/objectaddress.c get_object_oid_index
unsafe fn get_object_oid_index(_class_id: Oid) -> Oid {
    InvalidOid
}
/// TODO(pg-port): catalog/objectaddress.c get_object_class_descr
unsafe fn get_object_class_descr(_class_id: Oid) -> *const c_char {
    c"".as_ptr()
}

/// TODO(pg-port): commands/event_trigger.c trackDroppedObjectsNeeded
unsafe fn trackDroppedObjectsNeeded() -> bool {
    false
}
/// TODO(pg-port): commands/event_trigger.c EventTriggerSupportsObject
unsafe fn EventTriggerSupportsObject(_object: *const ObjectAddress) -> bool {
    false
}
/// TODO(pg-port): commands/event_trigger.c EventTriggerSQLDropAddObject
unsafe fn EventTriggerSQLDropAddObject(_object: *const ObjectAddress, _original: bool, _normal: bool) {}

/// TODO(pg-port): catalog/pg_shdepend.c deleteSharedDependencyRecordsFor
unsafe fn deleteSharedDependencyRecordsFor(_classId: Oid, _objectId: Oid, _objectSubId: int32) {}

/// TODO(pg-port): commands/comment.c DeleteComments
unsafe fn DeleteComments(_objectId: Oid, _classId: Oid, _objectSubId: int32) {}
/// TODO(pg-port): commands/seclabel.c DeleteSecurityLabel
unsafe fn DeleteSecurityLabel(_object: *const ObjectAddress) {}

/// TODO(pg-port): access/transam/xact.c CommandCounterIncrement
unsafe fn CommandCounterIncrement() {}

/// TODO(pg-port): catalog/indexing.c CatalogTupleDelete
unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut crate::storage::itemptr::ItemPointerData) {}

/// TODO(pg-port): utils/cache/syscache.c
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    null_mut()
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {}
unsafe fn SearchSysCacheExists1(_cacheId: c_int, _key1: Datum) -> bool {
    false
}

/// TODO(pg-port): catalog/pg_depend.c recordMultipleDependencies
unsafe fn recordMultipleDependencies(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _nreferenced: c_int,
    _behavior: DependencyType,
) {
}
/// TODO(pg-port): catalog/pg_depend.c recordDependencyOn
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: DependencyType,
) {
}

/// TODO(pg-port): storage/lmgr/lmgr.c lock helpers
unsafe fn LockRelationOid(_relid: Oid, _lockmode: c_int) {}
unsafe fn UnlockRelationOid(_relid: Oid, _lockmode: c_int) {}
unsafe fn LockSharedObject(_classId: Oid, _objectId: Oid, _objsubId: u16, _lockmode: c_int) {}
unsafe fn LockDatabaseObject(_classId: Oid, _objectId: Oid, _objsubId: u16, _lockmode: c_int) {}
unsafe fn UnlockDatabaseObject(_classId: Oid, _objectId: Oid, _objsubId: u16, _lockmode: c_int) {}

/// TODO(pg-port): tcop/postgres.c check_stack_depth
unsafe fn check_stack_depth() {}

/// TODO(pg-port): utils/error message-level test
unsafe fn message_level_is_interesting(_elevel: c_int) -> bool {
    true
}

/// TODO(pg-port): catalog/objectaccess.h InvokeObjectDropHookArg
unsafe fn InvokeObjectDropHookArg(_classId: Oid, _objectId: Oid, _objectSubId: int32, _dropflags: c_int) {}

// --- object deletion subroutines that live in other .c files ---
/// TODO(pg-port): catalog/index.c index_drop
unsafe fn index_drop(_indexId: Oid, _concurrent: bool, _concurrent_lock_mode: bool) {}
/// TODO(pg-port): catalog/heap.c RemoveAttributeById
unsafe fn RemoveAttributeById(_relid: Oid, _attnum: AttrNumber) {}
/// TODO(pg-port): catalog/heap.c heap_drop_with_catalog
unsafe fn heap_drop_with_catalog(_relid: Oid) {}
/// TODO(pg-port): commands/sequence.c DeleteSequenceTuple
unsafe fn DeleteSequenceTuple(_seqid: Oid) {}
/// TODO(pg-port): commands/functioncmds.c RemoveFunctionById
unsafe fn RemoveFunctionById(_funcOid: Oid) {}
/// TODO(pg-port): commands/typecmds.c RemoveTypeById
unsafe fn RemoveTypeById(_typeOid: Oid) {}
/// TODO(pg-port): catalog/pg_constraint.c RemoveConstraintById
unsafe fn RemoveConstraintById(_conId: Oid) {}
/// TODO(pg-port): catalog/heap.c RemoveAttrDefaultById
unsafe fn RemoveAttrDefaultById(_attrdefId: Oid) {}
/// TODO(pg-port): catalog/pg_largeobject.c LargeObjectDrop
unsafe fn LargeObjectDrop(_loid: Oid) {}
/// TODO(pg-port): commands/operatorcmds.c RemoveOperatorById
unsafe fn RemoveOperatorById(_operOid: Oid) {}
/// TODO(pg-port): rewrite/rewriteRemove.c RemoveRewriteRuleById
unsafe fn RemoveRewriteRuleById(_ruleOid: Oid) {}
/// TODO(pg-port): commands/trigger.c RemoveTriggerById
unsafe fn RemoveTriggerById(_trigOid: Oid) {}
/// TODO(pg-port): statistics/statscmds.c RemoveStatisticsById
unsafe fn RemoveStatisticsById(_statsOid: Oid) {}
/// TODO(pg-port): commands/tsearchcmds.c RemoveTSConfigurationById
unsafe fn RemoveTSConfigurationById(_cfgId: Oid) {}
/// TODO(pg-port): commands/extension.c RemoveExtensionById
unsafe fn RemoveExtensionById(_extId: Oid) {}
/// TODO(pg-port): commands/policy.c RemovePolicyById
unsafe fn RemovePolicyById(_policy_id: Oid) {}
/// TODO(pg-port): commands/publicationcmds.c RemovePublicationSchemaById
unsafe fn RemovePublicationSchemaById(_psoid: Oid) {}
/// TODO(pg-port): commands/publicationcmds.c RemovePublicationRelById
unsafe fn RemovePublicationRelById(_proid: Oid) {}
/// TODO(pg-port): commands/publicationcmds.c RemovePublicationById
unsafe fn RemovePublicationById(_pubid: Oid) {}

/// TODO(pg-port): utils/cache/lsyscache.c get_rel_relkind
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    0
}
/// TODO(pg-port): utils/cache/lsyscache.c get_typ_typrelid
unsafe fn get_typ_typrelid(_typid: Oid) -> Oid {
    InvalidOid
}
/// TODO(pg-port): utils/cache/lsyscache.c getBaseType
unsafe fn getBaseType(_typid: Oid) -> Oid {
    InvalidOid
}
/// TODO(pg-port): nodes/nodeFuncs.c exprType
unsafe fn exprType(_expr: *const Node) -> Oid {
    InvalidOid
}
/// TODO(pg-port): utils/cache/typcache.c get_expr_result_tupdesc
unsafe fn get_expr_result_tupdesc(
    _expr: *mut Node,
    _noError: bool,
) -> crate::access::common::tupdesc::TupleDesc {
    null_mut()
}

/// TODO(pg-port): nodes/nodeFuncs.c expression_tree_walker
unsafe fn expression_tree_walker(
    _node: *mut Node,
    _walker: unsafe fn(*mut Node, *mut find_expr_references_context) -> bool,
    _context: *mut find_expr_references_context,
) -> bool {
    false
}
/// TODO(pg-port): nodes/nodeFuncs.c query_tree_walker
unsafe fn query_tree_walker(
    _query: *mut Query,
    _walker: unsafe fn(*mut Node, *mut find_expr_references_context) -> bool,
    _context: *mut find_expr_references_context,
    _flags: c_int,
) -> bool {
    false
}
const QTW_IGNORE_JOINALIASES: c_int = 0x04;
const QTW_EXAMINE_SORTGROUP: c_int = 0x40;

// --- syscache identifiers used by the Const walker ---
const PROCOID: c_int = 0;
const OPEROID: c_int = 0;
const RELOID: c_int = 0;
const TYPEOID: c_int = 0;
const COLLOID: c_int = 0;
const TSCONFIGOID: c_int = 0;
const TSDICTOID: c_int = 0;
const NAMESPACEOID: c_int = 0;

const MAX_REPORTED_DEPS: c_int = 100;

// ----------------------------------------------------------------
//   list/rangetable helpers (parser/parsetree.h)
// ----------------------------------------------------------------
/// rt_fetch(rangetable_index, rangetable): the RangeTblEntry at 1-based index.
unsafe fn rt_fetch(rangetable_index: c_int, rangetable: *mut List) -> *mut RangeTblEntry {
    list_nth(rangetable, rangetable_index - 1) as *mut RangeTblEntry
}

/*
 * Go through the objects given running the final actions on them, and execute
 * the actual deletion.
 */
unsafe fn deleteObjectsInList(targetObjects: *mut ObjectAddresses, depRel: *mut Relation, flags: c_int) {
    let mut i: c_int;

    /*
     * Keep track of objects for event triggers, if necessary.
     */
    if trackDroppedObjectsNeeded() && (flags & PERFORM_DELETION_INTERNAL) == 0 {
        i = 0;
        while i < (*targetObjects).numrefs {
            let thisobj: *const ObjectAddress = (*targetObjects).refs.add(i as usize);
            let extra: *const ObjectAddressExtra = (*targetObjects).extras.add(i as usize);
            let mut original = false;
            let mut normal = false;

            if (*extra).flags & DEPFLAG_ORIGINAL != 0 {
                original = true;
            }
            if (*extra).flags & DEPFLAG_NORMAL != 0 {
                normal = true;
            }
            if (*extra).flags & DEPFLAG_REVERSE != 0 {
                normal = true;
            }

            if EventTriggerSupportsObject(thisobj) {
                EventTriggerSQLDropAddObject(thisobj, original, normal);
            }
            i += 1;
        }
    }

    /*
     * Delete all the objects in the proper order, except that if told to, we
     * should skip the original object(s).
     */
    i = 0;
    while i < (*targetObjects).numrefs {
        let thisobj: *mut ObjectAddress = (*targetObjects).refs.add(i as usize);
        let thisextra: *mut ObjectAddressExtra = (*targetObjects).extras.add(i as usize);

        if (flags & PERFORM_DELETION_SKIP_ORIGINAL) != 0
            && ((*thisextra).flags & DEPFLAG_ORIGINAL) != 0
        {
            i += 1;
            continue;
        }

        deleteOneObject(thisobj, depRel, flags);
        i += 1;
    }
}

/*
 * performDeletion: attempt to drop the specified object.  If CASCADE
 * behavior is specified, also drop any dependent objects (recursively).
 * If RESTRICT behavior is specified, error out if there are any dependent
 * objects, except for those that should be implicitly dropped anyway
 * according to the dependency type.
 *
 * See the C source for the full description of the flags argument.
 */
pub unsafe fn performDeletion(object: *const ObjectAddress, behavior: DropBehavior, flags: c_int) {
    let mut depRel: Relation;
    let targetObjects: *mut ObjectAddresses;

    /*
     * We save some cycles by opening pg_depend just once and passing the
     * Relation pointer down to all the recursive deletion steps.
     */
    depRel = table_open(DependRelationId, RowExclusiveLock);

    /*
     * Acquire deletion lock on the target object.  (Ideally the caller has
     * done this already, but many places are sloppy about it.)
     */
    AcquireDeletionLock(object, 0);

    /*
     * Construct a list of objects to delete (ie, the given object plus
     * everything directly or indirectly dependent on it).
     */
    targetObjects = new_object_addresses();

    findDependentObjects(
        object,
        DEPFLAG_ORIGINAL,
        flags,
        null_mut(), /* empty stack */
        targetObjects,
        null(), /* no pendingObjects */
        &mut depRel,
    );

    /*
     * Check if deletion is allowed, and report about cascaded deletes.
     */
    reportDependentObjects(targetObjects, behavior, flags, object);

    /* do the deed */
    deleteObjectsInList(targetObjects, &mut depRel, flags);

    /* And clean up */
    free_object_addresses(targetObjects);

    table_close(depRel, RowExclusiveLock);
}

/*
 * performMultipleDeletions: Similar to performDeletion, but act on multiple
 * objects at once.
 */
pub unsafe fn performMultipleDeletions(
    objects: *const ObjectAddresses,
    behavior: DropBehavior,
    flags: c_int,
) {
    let mut depRel: Relation;
    let targetObjects: *mut ObjectAddresses;
    let mut i: c_int;

    /* No work if no objects... */
    if (*objects).numrefs <= 0 {
        return;
    }

    /*
     * We save some cycles by opening pg_depend just once and passing the
     * Relation pointer down to all the recursive deletion steps.
     */
    depRel = table_open(DependRelationId, RowExclusiveLock);

    /*
     * Construct a list of objects to delete (ie, the given objects plus
     * everything directly or indirectly dependent on them).
     */
    targetObjects = new_object_addresses();

    i = 0;
    while i < (*objects).numrefs {
        let thisobj: *const ObjectAddress = (*objects).refs.add(i as usize);

        /*
         * Acquire deletion lock on each target object.  (Ideally the caller
         * has done this already, but many places are sloppy about it.)
         */
        AcquireDeletionLock(thisobj, flags);

        findDependentObjects(
            thisobj,
            DEPFLAG_ORIGINAL,
            flags,
            null_mut(), /* empty stack */
            targetObjects,
            objects,
            &mut depRel,
        );
        i += 1;
    }

    /*
     * Check if deletion is allowed, and report about cascaded deletes.
     *
     * If there's exactly one object being deleted, report it the same way as
     * in performDeletion(), else we have to be vaguer.
     */
    reportDependentObjects(
        targetObjects,
        behavior,
        flags,
        if (*objects).numrefs == 1 { (*objects).refs } else { null() },
    );

    /* do the deed */
    deleteObjectsInList(targetObjects, &mut depRel, flags);

    /* And clean up */
    free_object_addresses(targetObjects);

    table_close(depRel, RowExclusiveLock);
}

// ----------------------------------------------------------------
//   Additional cross-module stubs / globals used below.
// ----------------------------------------------------------------

// pg_depend index OIDs (genbki-assigned).  TODO(pg-port): catalog/pg_depend.h
const DependDependerIndexId: Oid = 2673;
const DependReferenceIndexId: Oid = 2674;
// pg_init_privs index OID.  TODO(pg-port): catalog/pg_init_privs.h
const InitPrivsObjIndexId: Oid = 3396;

/// TODO(pg-port): commands/extension.c globals controlling extension creation.
static mut creating_extension: bool = false;
static mut CurrentExtensionObject: Oid = InvalidOid;

/// TODO(pg-port): utils/sort qsort wrapper (matches libc qsort semantics).
unsafe fn qsort(
    _base: *mut c_void,
    _nmemb: usize,
    _size: usize,
    _compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
) {
}

/*
 * findDependentObjects - find all objects that depend on 'object'
 *
 * (see the C source for the full description of the algorithm and arguments)
 */
unsafe fn findDependentObjects(
    object: *const ObjectAddress,
    mut objflags: c_int,
    flags: c_int,
    stack: *mut ObjectAddressStack,
    targetObjects: *mut ObjectAddresses,
    pendingObjects: *const ObjectAddresses,
    depRel: *mut Relation,
) {
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let mut nkeys: c_int;
    let mut scan: SysScanDesc;
    let mut tup: HeapTuple;
    let mut otherObject: ObjectAddress = core::mem::zeroed();
    let mut owningObject: ObjectAddress;
    let mut partitionObject: ObjectAddress;
    let dependentObjects: *mut ObjectAddressAndFlags;
    let mut numDependentObjects: c_int;
    let mut maxDependentObjects: c_int;
    let mut mystack: ObjectAddressStack = core::mem::zeroed();
    let mut extra: ObjectAddressExtra = core::mem::zeroed();

    /*
     * If the target object is already being visited in an outer recursion
     * level, just report the current objflags back to that level and exit.
     * This is needed to avoid infinite recursion in the face of circular
     * dependencies.
     */
    if stack_address_present_add_flags(object, objflags, stack) {
        return;
    }

    /*
     * since this function recurses, it could be driven to stack overflow,
     * because of the deep dependency tree, not only due to dependency loops.
     */
    check_stack_depth();

    /*
     * It's also possible that the target object has already been completely
     * processed and put into targetObjects.  If so, again we just add the
     * specified objflags to its entry and return.
     */
    if object_address_present_add_flags(object, objflags, targetObjects) {
        return;
    }

    /*
     * If the target object is pinned, we can just error out immediately; it
     * won't have any objects recorded as depending on it.
     */
    if IsPinnedObject((*object).classId, (*object).objectId) {
        ereport!(
            ERROR,
            errmsg!(
                "cannot drop {} because it is required by the database system",
                CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST) */
    }

    /*
     * The target object might be internally dependent on some other object
     * (its "owner"), and/or be a member of an extension (also considered its
     * owner).  If so, and if we aren't recursing from the owning object, we
     * have to transform this deletion request into a deletion request of the
     * owning object.
     */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    if (*object).objectSubId != 0 {
        /* Consider only dependencies of this sub-object */
        ScanKeyInit(
            &mut key[2],
            Anum_pg_depend_objsubid,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum((*object).objectSubId),
        );
        nkeys = 3;
    } else {
        /* Consider dependencies of this object and any sub-objects it has */
        nkeys = 2;
    }

    scan = systable_beginscan(
        *depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        nkeys,
        key.as_mut_ptr(),
    );

    /* initialize variables that loop may fill */
    owningObject = core::mem::zeroed();
    partitionObject = core::mem::zeroed();

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }
        let foundDep: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;

        otherObject.classId = (*foundDep).refclassid;
        otherObject.objectId = (*foundDep).refobjid;
        otherObject.objectSubId = (*foundDep).refobjsubid;

        /*
         * When scanning dependencies of a whole object, we may find rows
         * linking sub-objects of the object to the object itself.  We must
         * ignore such rows to avoid infinite recursion.
         */
        if otherObject.classId == (*object).classId
            && otherObject.objectId == (*object).objectId
            && (*object).objectSubId == 0
        {
            continue;
        }

        match (*foundDep).deptype as u8 as char {
            c if c == DEPENDENCY_NORMAL as u8 as char
                || c == DEPENDENCY_AUTO as u8 as char
                || c == DEPENDENCY_AUTO_EXTENSION as u8 as char =>
            {
                /* no problem */
            }

            c if c == DEPENDENCY_EXTENSION as u8 as char
                || c == DEPENDENCY_INTERNAL as u8 as char =>
            {
                /*
                 * For DEPENDENCY_EXTENSION, first apply the special cases that
                 * may let us ignore the dependency; otherwise treat it like an
                 * internal dependency.
                 */
                if c == DEPENDENCY_EXTENSION as u8 as char {
                    /*
                     * If told to, ignore EXTENSION dependencies altogether.
                     */
                    if flags & PERFORM_DELETION_SKIP_EXTENSIONS != 0 {
                        continue;
                    }

                    /*
                     * If the other object is the extension currently being
                     * created/altered, ignore this dependency and continue with
                     * the deletion.
                     */
                    if creating_extension
                        && otherObject.classId == ExtensionRelationId
                        && otherObject.objectId == CurrentExtensionObject
                    {
                        continue;
                    }

                    /* Otherwise, treat this like an internal dependency */
                }

                /*
                 * This object is part of the internal implementation of
                 * another object, or is part of the extension that is the
                 * other object.
                 *
                 * 1. At the outermost recursion level, we must disallow the
                 * DROP.  However, if the owning object is listed in
                 * pendingObjects, just release the caller's lock and return.
                 */
                if stack.is_null() {
                    if !pendingObjects.is_null()
                        && object_address_present(&otherObject, pendingObjects)
                    {
                        systable_endscan(scan);
                        /* need to release caller's lock; see notes below */
                        ReleaseDeletionLock(object);
                        return;
                    }

                    /*
                     * We postpone actually issuing the error message until
                     * after this loop.  Prefer to complain about EXTENSION.
                     */
                    if !OidIsValid(owningObject.classId)
                        || (*foundDep).deptype as u8 as char == DEPENDENCY_EXTENSION as u8 as char
                    {
                        owningObject = otherObject;
                    }
                    continue;
                }

                /*
                 * 2. When recursing from the other end of this dependency,
                 * it's okay to continue with the deletion.
                 */
                if stack_address_present_add_flags(&otherObject, 0, stack) {
                    continue;
                }

                /*
                 * 3. Not all the owning objects have been visited, so
                 * transform this deletion request into a delete of this
                 * owning object.
                 */
                ReleaseDeletionLock(object);
                AcquireDeletionLock(&otherObject, 0);

                /*
                 * The owning object might have been deleted while we waited to
                 * lock it; if so, neither it nor the current object are
                 * interesting anymore.
                 */
                if !systable_recheck_tuple(scan, tup as *mut c_void) {
                    systable_endscan(scan);
                    ReleaseDeletionLock(&otherObject);
                    return;
                }

                /*
                 * One way or the other, we're done with the scan; might as
                 * well close it down before recursing.
                 */
                systable_endscan(scan);

                /*
                 * Okay, recurse to the owning object instead of proceeding.
                 */
                findDependentObjects(
                    &otherObject,
                    DEPFLAG_REVERSE,
                    flags,
                    stack,
                    targetObjects,
                    pendingObjects,
                    depRel,
                );

                /*
                 * The current target object should have been added to
                 * targetObjects while processing the owning object.
                 */
                if !object_address_present_add_flags(object, objflags, targetObjects) {
                    elog!(
                        ERROR,
                        "deletion of owning object {} failed to delete {}",
                        CStr::from_ptr(getObjectDescription(&otherObject, false)).to_string_lossy(),
                        CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy()
                    );
                }

                /* And we're done here. */
                return;
            }

            c if c == DEPENDENCY_PARTITION_PRI as u8 as char => {
                /*
                 * Remember that this object has a partition-type dependency.
                 */
                objflags |= DEPFLAG_IS_PART;

                /*
                 * Also remember the primary partition owner, for error
                 * messages.
                 */
                partitionObject = otherObject;
            }

            c if c == DEPENDENCY_PARTITION_SEC as u8 as char => {
                /*
                 * Only use secondary partition owners in error messages if we
                 * find no primary owner.
                 */
                if objflags & DEPFLAG_IS_PART == 0 {
                    partitionObject = otherObject;
                }

                /*
                 * Remember that this object has a partition-type dependency.
                 */
                objflags |= DEPFLAG_IS_PART;
            }

            _ => {
                elog!(
                    ERROR,
                    "unrecognized dependency type '{}' for {}",
                    (*foundDep).deptype as u8 as char,
                    CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy()
                );
            }
        }
    }

    systable_endscan(scan);

    /*
     * If we found an INTERNAL or EXTENSION dependency when we're at outer
     * level, complain about it now.  If we also found a PARTITION dependency,
     * we prefer to report the PARTITION dependency.
     */
    if OidIsValid(owningObject.classId) {
        let otherObjDesc: *mut c_char;

        if OidIsValid(partitionObject.classId) {
            otherObjDesc = getObjectDescription(&partitionObject, false);
        } else {
            otherObjDesc = getObjectDescription(&owningObject, false);
        }

        ereport!(
            ERROR,
            errmsg!(
                "cannot drop {} because {} requires it",
                CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy(),
                CStr::from_ptr(otherObjDesc).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST) */
        /* C also: errhint("You can drop %s instead.", otherObjDesc) */
    }

    /*
     * Next, identify all objects that directly depend on the current object.
     * To ensure predictable deletion order, we collect them up in
     * dependentObjects and sort the list before actually recursing.
     */
    maxDependentObjects = 128; /* arbitrary initial allocation */
    dependentObjects = palloc(
        maxDependentObjects as usize * core::mem::size_of::<ObjectAddressAndFlags>(),
    ) as *mut ObjectAddressAndFlags;
    numDependentObjects = 0;

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    if (*object).objectSubId != 0 {
        ScanKeyInit(
            &mut key[2],
            Anum_pg_depend_refobjsubid,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum((*object).objectSubId),
        );
        nkeys = 3;
    } else {
        nkeys = 2;
    }

    scan = systable_beginscan(
        *depRel,
        DependReferenceIndexId,
        true,
        null_mut(),
        nkeys,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }
        let foundDep: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;
        let subflags: c_int;

        otherObject.classId = (*foundDep).classid;
        otherObject.objectId = (*foundDep).objid;
        otherObject.objectSubId = (*foundDep).objsubid;

        /*
         * If what we found is a sub-object of the current object, just ignore
         * it.
         */
        if otherObject.classId == (*object).classId
            && otherObject.objectId == (*object).objectId
            && (*object).objectSubId == 0
        {
            continue;
        }

        /*
         * Must lock the dependent object before recursing to it.
         */
        AcquireDeletionLock(&otherObject, 0);

        /*
         * The dependent object might have been deleted while we waited to
         * lock it; if so, we don't need to do anything more with it.
         */
        if !systable_recheck_tuple(scan, tup as *mut c_void) {
            /* release the now-useless lock */
            ReleaseDeletionLock(&otherObject);
            /* and continue scanning for dependencies */
            continue;
        }

        /*
         * We do need to delete it, so identify objflags to be passed down,
         * which depend on the dependency type.
         */
        let dt = (*foundDep).deptype as u8 as char;
        if dt == DEPENDENCY_NORMAL as u8 as char {
            subflags = DEPFLAG_NORMAL;
        } else if dt == DEPENDENCY_AUTO as u8 as char
            || dt == DEPENDENCY_AUTO_EXTENSION as u8 as char
        {
            subflags = DEPFLAG_AUTO;
        } else if dt == DEPENDENCY_INTERNAL as u8 as char {
            subflags = DEPFLAG_INTERNAL;
        } else if dt == DEPENDENCY_PARTITION_PRI as u8 as char
            || dt == DEPENDENCY_PARTITION_SEC as u8 as char
        {
            subflags = DEPFLAG_PARTITION;
        } else if dt == DEPENDENCY_EXTENSION as u8 as char {
            subflags = DEPFLAG_EXTENSION;
        } else {
            elog!(
                ERROR,
                "unrecognized dependency type '{}' for {}",
                (*foundDep).deptype as u8 as char,
                CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy()
            );
            subflags = 0; /* keep compiler quiet */
        }

        /* And add it to the pending-objects list */
        if numDependentObjects >= maxDependentObjects {
            /* enlarge array if needed */
            maxDependentObjects *= 2;
            let _ = repalloc(
                dependentObjects as *mut c_void,
                maxDependentObjects as usize * core::mem::size_of::<ObjectAddressAndFlags>(),
            );
        }

        (*dependentObjects.add(numDependentObjects as usize)).obj = otherObject;
        (*dependentObjects.add(numDependentObjects as usize)).subflags = subflags;
        numDependentObjects += 1;
    }

    systable_endscan(scan);

    /*
     * Now we can sort the dependent objects into a stable visitation order.
     * It's safe to use object_address_comparator here since the obj field is
     * first within ObjectAddressAndFlags.
     */
    if numDependentObjects > 1 {
        qsort(
            dependentObjects as *mut c_void,
            numDependentObjects as usize,
            core::mem::size_of::<ObjectAddressAndFlags>(),
            object_address_comparator,
        );
    }

    /*
     * Now recurse to the dependent objects.  We must visit them first since
     * they have to be deleted before the current object.
     */
    mystack.object = object; /* set up a new stack level */
    mystack.flags = objflags;
    mystack.next = stack;

    let mut i: c_int = 0;
    while i < numDependentObjects {
        let depObj: *mut ObjectAddressAndFlags = dependentObjects.add(i as usize);

        findDependentObjects(
            &(*depObj).obj,
            (*depObj).subflags,
            flags,
            &mut mystack,
            targetObjects,
            pendingObjects,
            depRel,
        );
        i += 1;
    }

    pfree(dependentObjects as *mut c_void);

    /*
     * Finally, we can add the target object to targetObjects.  Be careful to
     * include any flags that were passed back down to us from inner recursion
     * levels.
     */
    extra.flags = mystack.flags;
    if extra.flags & DEPFLAG_IS_PART != 0 {
        extra.dependee = partitionObject;
    } else if !stack.is_null() {
        extra.dependee = *(*stack).object;
    } else {
        extra.dependee = core::mem::zeroed();
    }
    add_exact_object_address_extra(object, &extra, targetObjects);
}

/*
 * reportDependentObjects - report about dependencies, and fail if RESTRICT
 *
 * Tell the user about dependent objects that we are going to delete
 * (or would need to delete, but are prevented by RESTRICT mode);
 * then error out if there are any and it's not CASCADE mode.
 */
unsafe fn reportDependentObjects(
    targetObjects: *const ObjectAddresses,
    behavior: DropBehavior,
    flags: c_int,
    origObject: *const ObjectAddress,
) {
    let msglevel: c_int = if flags & PERFORM_DELETION_QUIETLY != 0 {
        DEBUG2
    } else {
        NOTICE
    };
    let mut ok: bool = true;
    let mut clientdetail: StringInfoData = core::mem::zeroed();
    let mut logdetail: StringInfoData = core::mem::zeroed();
    let mut numReportedClient: c_int = 0;
    let mut numNotReportedClient: c_int = 0;
    let mut i: c_int;

    /*
     * If we need to delete any partition-dependent objects, make sure that
     * we're deleting at least one of their partition dependencies, too.
     */
    i = 0;
    while i < (*targetObjects).numrefs {
        let extra: *const ObjectAddressExtra = (*targetObjects).extras.add(i as usize);

        if (*extra).flags & DEPFLAG_IS_PART != 0 && (*extra).flags & DEPFLAG_PARTITION == 0 {
            let object: *const ObjectAddress = (*targetObjects).refs.add(i as usize);
            let otherObjDesc: *mut c_char = getObjectDescription(&(*extra).dependee, false);

            ereport!(
                ERROR,
                errmsg!(
                    "cannot drop {} because {} requires it",
                    CStr::from_ptr(getObjectDescription(object, false)).to_string_lossy(),
                    CStr::from_ptr(otherObjDesc).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST) */
            /* C also: errhint("You can drop %s instead.", otherObjDesc) */
        }
        i += 1;
    }

    /*
     * If no error is to be thrown, and the msglevel is too low to be shown to
     * either client or server log, there's no need to do any of the rest of
     * the work.
     */
    if behavior == DROP_CASCADE && !message_level_is_interesting(msglevel) {
        return;
    }

    /*
     * We limit the number of dependencies reported to the client to
     * MAX_REPORTED_DEPS, since client software may not deal well with
     * enormous error strings.  The server log always gets a full report.
     */

    initStringInfo(&mut clientdetail);
    initStringInfo(&mut logdetail);

    /*
     * We process the list back to front (ie, in dependency order not deletion
     * order), since this makes for a more understandable display.
     */
    i = (*targetObjects).numrefs - 1;
    while i >= 0 {
        let obj: *const ObjectAddress = (*targetObjects).refs.add(i as usize);
        let extra: *const ObjectAddressExtra = (*targetObjects).extras.add(i as usize);
        let objDesc: *mut c_char;

        /* Ignore the original deletion target(s) */
        if (*extra).flags & DEPFLAG_ORIGINAL != 0 {
            i -= 1;
            continue;
        }

        /* Also ignore sub-objects; we'll report the whole object elsewhere */
        if (*extra).flags & DEPFLAG_SUBOBJECT != 0 {
            i -= 1;
            continue;
        }

        objDesc = getObjectDescription(obj, false);

        /* An object being dropped concurrently doesn't need to be reported */
        if objDesc.is_null() {
            i -= 1;
            continue;
        }

        /*
         * If, at any stage of the recursive search, we reached the object via
         * an AUTO, INTERNAL, PARTITION, or EXTENSION dependency, then it's
         * okay to delete it even in RESTRICT mode.
         */
        if (*extra).flags
            & (DEPFLAG_AUTO | DEPFLAG_INTERNAL | DEPFLAG_PARTITION | DEPFLAG_EXTENSION)
            != 0
        {
            /*
             * auto-cascades are reported at DEBUG2, not msglevel.
             */
            ereport!(
                DEBUG2,
                errmsg_internal!(
                    "drop auto-cascades to {}",
                    CStr::from_ptr(objDesc).to_string_lossy()
                )
            );
        } else if behavior == DROP_RESTRICT {
            let otherDesc: *mut c_char = getObjectDescription(&(*extra).dependee, false);

            if !otherDesc.is_null() {
                if numReportedClient < MAX_REPORTED_DEPS {
                    /* separate entries with a newline */
                    if clientdetail.len != 0 {
                        appendStringInfoChar(&mut clientdetail, b'\n' as c_char);
                    }
                    appendStringInfo!(
                        &mut clientdetail,
                        "{} depends on {}",
                        CStr::from_ptr(objDesc).to_string_lossy(),
                        CStr::from_ptr(otherDesc).to_string_lossy()
                    );
                    numReportedClient += 1;
                } else {
                    numNotReportedClient += 1;
                }
                /* separate entries with a newline */
                if logdetail.len != 0 {
                    appendStringInfoChar(&mut logdetail, b'\n' as c_char);
                }
                appendStringInfo!(
                    &mut logdetail,
                    "{} depends on {}",
                    CStr::from_ptr(objDesc).to_string_lossy(),
                    CStr::from_ptr(otherDesc).to_string_lossy()
                );
                pfree(otherDesc as *mut c_void);
            } else {
                numNotReportedClient += 1;
            }
            ok = false;
        } else {
            if numReportedClient < MAX_REPORTED_DEPS {
                /* separate entries with a newline */
                if clientdetail.len != 0 {
                    appendStringInfoChar(&mut clientdetail, b'\n' as c_char);
                }
                appendStringInfo!(
                    &mut clientdetail,
                    "drop cascades to {}",
                    CStr::from_ptr(objDesc).to_string_lossy()
                );
                numReportedClient += 1;
            } else {
                numNotReportedClient += 1;
            }
            /* separate entries with a newline */
            if logdetail.len != 0 {
                appendStringInfoChar(&mut logdetail, b'\n' as c_char);
            }
            appendStringInfo!(
                &mut logdetail,
                "drop cascades to {}",
                CStr::from_ptr(objDesc).to_string_lossy()
            );
        }

        pfree(objDesc as *mut c_void);
        i -= 1;
    }

    if numNotReportedClient > 0 {
        appendStringInfo!(
            &mut clientdetail,
            "\nand {} other objects (see server log for list)",
            numNotReportedClient
        );
        /* C also: ngettext singular/plural on numNotReportedClient */
    }

    if !ok {
        if !origObject.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot drop {} because other objects depend on it",
                    CStr::from_ptr(getObjectDescription(origObject, false)).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST) */
            /* C also: errdetail_internal("%s", clientdetail.data) */
            /* C also: errdetail_log("%s", logdetail.data) */
            /* C also: errhint("Use DROP ... CASCADE to drop the dependent objects too.") */
        } else {
            ereport!(
                ERROR,
                errmsg!("cannot drop desired object(s) because other objects depend on them")
            );
            /* C also: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST) */
            /* C also: errdetail_internal("%s", clientdetail.data) */
            /* C also: errdetail_log("%s", logdetail.data) */
            /* C also: errhint("Use DROP ... CASCADE to drop the dependent objects too.") */
        }
    } else if numReportedClient > 1 {
        // C uses errmsg_plural(); the count is always > 1 here so plural form.
        ereport!(
            msglevel,
            errmsg!(
                "drop cascades to {} other objects",
                numReportedClient + numNotReportedClient
            )
        );
        /* C also: errdetail_internal("%s", clientdetail.data) */
        /* C also: errdetail_log("%s", logdetail.data) */
    } else if numReportedClient == 1 {
        /* we just use the single item as-is */
        ereport!(
            msglevel,
            errmsg_internal!(
                "{}",
                CStr::from_ptr(clientdetail.data).to_string_lossy()
            )
        );
    }

    pfree(clientdetail.data as *mut c_void);
    pfree(logdetail.data as *mut c_void);
}

/*
 * Drop an object by OID.  Works for most catalogs, if no special processing
 * is needed.
 */
unsafe fn DropObjectById(object: *const ObjectAddress) {
    let cacheId: c_int;
    let rel: Relation;
    let tup: HeapTuple;

    cacheId = get_object_catcache_oid((*object).classId);

    rel = table_open((*object).classId, RowExclusiveLock);

    /*
     * Use the system cache for the oid column, if one exists.
     */
    if cacheId >= 0 {
        tup = SearchSysCache1(cacheId, ObjectIdGetDatum((*object).objectId));
        if !HeapTupleIsValid(tup) {
            elog!(
                ERROR,
                "cache lookup failed for {} {}",
                CStr::from_ptr(get_object_class_descr((*object).classId)).to_string_lossy(),
                (*object).objectId
            );
        }

        CatalogTupleDelete(rel, &mut (*tup).t_self);

        ReleaseSysCache(tup);
    } else {
        let mut skey: [ScanKeyData; 1] = core::mem::zeroed();
        let scan: SysScanDesc;

        ScanKeyInit(
            &mut skey[0],
            get_object_attnum_oid((*object).classId),
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum((*object).objectId),
        );

        scan = systable_beginscan(
            rel,
            get_object_oid_index((*object).classId),
            true,
            null_mut(),
            1,
            skey.as_mut_ptr(),
        );

        /* we expect exactly one match */
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            elog!(
                ERROR,
                "could not find tuple for {} {}",
                CStr::from_ptr(get_object_class_descr((*object).classId)).to_string_lossy(),
                (*object).objectId
            );
        }

        CatalogTupleDelete(rel, &mut (*tup).t_self);

        systable_endscan(scan);
    }

    table_close(rel, RowExclusiveLock);
}

/*
 * deleteOneObject: delete a single object for performDeletion.
 *
 * *depRel is the already-open pg_depend relation.
 */
unsafe fn deleteOneObject(object: *const ObjectAddress, depRel: *mut Relation, flags: c_int) {
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let nkeys: c_int;
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    /* DROP hook of the objects being removed */
    InvokeObjectDropHookArg(
        (*object).classId,
        (*object).objectId,
        (*object).objectSubId,
        flags,
    );

    /*
     * Close depRel if we are doing a drop concurrently.  The object deletion
     * subroutine will commit the current transaction, so we can't keep the
     * relation open across doDeletion().
     */
    if flags & PERFORM_DELETION_CONCURRENTLY != 0 {
        table_close(*depRel, RowExclusiveLock);
    }

    /*
     * Delete the object itself, in an object-type-dependent way.
     */
    doDeletion(object, flags);

    /*
     * Reopen depRel if we closed it above
     */
    if flags & PERFORM_DELETION_CONCURRENTLY != 0 {
        *depRel = table_open(DependRelationId, RowExclusiveLock);
    }

    /*
     * Now remove any pg_depend records that link from this object to others.
     * (Any records linking to this object should be gone already.)
     */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    if (*object).objectSubId != 0 {
        ScanKeyInit(
            &mut key[2],
            Anum_pg_depend_objsubid,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum((*object).objectSubId),
        );
        nkeys = 3;
    } else {
        nkeys = 2;
    }

    scan = systable_beginscan(
        *depRel,
        DependDependerIndexId,
        true,
        null_mut(),
        nkeys,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(tup) {
            break;
        }
        CatalogTupleDelete(*depRel, &mut (*tup).t_self);
    }

    systable_endscan(scan);

    /*
     * Delete shared dependency references related to this object.
     */
    deleteSharedDependencyRecordsFor(
        (*object).classId,
        (*object).objectId,
        (*object).objectSubId,
    );

    /*
     * Delete any comments, security labels, or initial privileges associated
     * with this object.
     */
    DeleteComments(
        (*object).objectId,
        (*object).classId,
        (*object).objectSubId,
    );
    DeleteSecurityLabel(object);
    DeleteInitPrivs(object);

    /*
     * CommandCounterIncrement here to ensure that preceding changes are all
     * visible to the next deletion step.
     */
    CommandCounterIncrement();

    /*
     * And we're done!
     */
}

/*
 * doDeletion: actually delete a single object
 */
unsafe fn doDeletion(object: *const ObjectAddress, flags: c_int) {
    match (*object).classId {
        RelationRelationId => {
            let relKind: c_char = get_rel_relkind((*object).objectId);

            if relKind == RELKIND_INDEX || relKind == RELKIND_PARTITIONED_INDEX {
                let concurrent: bool = (flags & PERFORM_DELETION_CONCURRENTLY) != 0;
                let concurrent_lock_mode: bool = (flags & PERFORM_DELETION_CONCURRENT_LOCK) != 0;

                Assert!((*object).objectSubId == 0);
                index_drop((*object).objectId, concurrent, concurrent_lock_mode);
            } else {
                if (*object).objectSubId != 0 {
                    RemoveAttributeById((*object).objectId, (*object).objectSubId as AttrNumber);
                } else {
                    heap_drop_with_catalog((*object).objectId);
                }
            }

            /*
             * for a sequence, in addition to dropping the heap, also delete
             * pg_sequence tuple
             */
            if relKind == RELKIND_SEQUENCE {
                DeleteSequenceTuple((*object).objectId);
            }
        }

        ProcedureRelationId => {
            RemoveFunctionById((*object).objectId);
        }

        TypeRelationId => {
            RemoveTypeById((*object).objectId);
        }

        ConstraintRelationId => {
            RemoveConstraintById((*object).objectId);
        }

        AttrDefaultRelationId => {
            RemoveAttrDefaultById((*object).objectId);
        }

        LargeObjectRelationId => {
            LargeObjectDrop((*object).objectId);
        }

        OperatorRelationId => {
            RemoveOperatorById((*object).objectId);
        }

        RewriteRelationId => {
            RemoveRewriteRuleById((*object).objectId);
        }

        TriggerRelationId => {
            RemoveTriggerById((*object).objectId);
        }

        StatisticExtRelationId => {
            RemoveStatisticsById((*object).objectId);
        }

        TSConfigRelationId => {
            RemoveTSConfigurationById((*object).objectId);
        }

        ExtensionRelationId => {
            RemoveExtensionById((*object).objectId);
        }

        PolicyRelationId => {
            RemovePolicyById((*object).objectId);
        }

        PublicationNamespaceRelationId => {
            RemovePublicationSchemaById((*object).objectId);
        }

        PublicationRelRelationId => {
            RemovePublicationRelById((*object).objectId);
        }

        PublicationRelationId => {
            RemovePublicationById((*object).objectId);
        }

        CastRelationId
        | CollationRelationId
        | ConversionRelationId
        | LanguageRelationId
        | OperatorClassRelationId
        | OperatorFamilyRelationId
        | AccessMethodRelationId
        | AccessMethodOperatorRelationId
        | AccessMethodProcedureRelationId
        | NamespaceRelationId
        | TSParserRelationId
        | TSDictionaryRelationId
        | TSTemplateRelationId
        | ForeignDataWrapperRelationId
        | ForeignServerRelationId
        | UserMappingRelationId
        | DefaultAclRelationId
        | EventTriggerRelationId
        | TransformRelationId
        | AuthMemRelationId => {
            DropObjectById(object);
        }

        /*
         * These global object types are not supported here.
         */
        AuthIdRelationId
        | DatabaseRelationId
        | TableSpaceRelationId
        | SubscriptionRelationId
        | ParameterAclRelationId => {
            elog!(ERROR, "global objects cannot be deleted by doDeletion");
        }

        _ => {
            elog!(ERROR, "unsupported object class: {}", (*object).classId);
        }
    }
}

/*
 * AcquireDeletionLock - acquire a suitable lock for deleting an object
 *
 * Accepts the same flags as performDeletion (though currently only
 * PERFORM_DELETION_CONCURRENTLY does anything).
 */
pub unsafe fn AcquireDeletionLock(object: *const ObjectAddress, flags: c_int) {
    if (*object).classId == RelationRelationId {
        /*
         * In DROP INDEX CONCURRENTLY, take only ShareUpdateExclusiveLock on
         * the index for the moment.  index_drop() will promote the lock once
         * it's safe to do so.  In all other cases we need full exclusive
         * lock.
         */
        if flags & PERFORM_DELETION_CONCURRENTLY != 0 {
            LockRelationOid((*object).objectId, ShareUpdateExclusiveLock);
        } else {
            LockRelationOid((*object).objectId, AccessExclusiveLock);
        }
    } else if (*object).classId == AuthMemRelationId {
        LockSharedObject(
            (*object).classId,
            (*object).objectId,
            0,
            AccessExclusiveLock,
        );
    } else {
        /* assume we should lock the whole object not a sub-object */
        LockDatabaseObject(
            (*object).classId,
            (*object).objectId,
            0,
            AccessExclusiveLock,
        );
    }
}

/*
 * ReleaseDeletionLock - release an object deletion lock
 *
 * Companion to AcquireDeletionLock.
 */
pub unsafe fn ReleaseDeletionLock(object: *const ObjectAddress) {
    if (*object).classId == RelationRelationId {
        UnlockRelationOid((*object).objectId, AccessExclusiveLock);
    } else {
        /* assume we should lock the whole object not a sub-object */
        UnlockDatabaseObject(
            (*object).classId,
            (*object).objectId,
            0,
            AccessExclusiveLock,
        );
    }
}

/*
 * recordDependencyOnExpr - find expression dependencies
 *
 * This is used to find the dependencies of rules, constraint expressions,
 * etc.
 */
pub unsafe fn recordDependencyOnExpr(
    depender: *const ObjectAddress,
    expr: *mut Node,
    rtable: *mut List,
    behavior: DependencyType,
) {
    let mut context: find_expr_references_context = core::mem::zeroed();

    context.addrs = new_object_addresses();

    /* Set up interpretation for Vars at varlevelsup = 0 */
    context.rtables = list_make1!(rtable);

    /* Scan the expression tree for referenceable objects */
    find_expr_references_walker(expr, &mut context);

    /* Remove any duplicates */
    eliminate_duplicate_dependencies(context.addrs);

    /* And record 'em */
    recordMultipleDependencies(
        depender,
        (*context.addrs).refs,
        (*context.addrs).numrefs,
        behavior,
    );

    free_object_addresses(context.addrs);
}

/*
 * recordDependencyOnSingleRelExpr - find expression dependencies
 *
 * As above, but only one relation is expected to be referenced (with
 * varno = 1 and varlevelsup = 0).
 */
pub unsafe fn recordDependencyOnSingleRelExpr(
    depender: *const ObjectAddress,
    expr: *mut Node,
    relId: Oid,
    behavior: DependencyType,
    self_behavior: DependencyType,
    reverse_self: bool,
) {
    let mut context: find_expr_references_context = core::mem::zeroed();
    let mut rte: RangeTblEntry = core::mem::zeroed();

    context.addrs = new_object_addresses();

    /* We gin up a rather bogus rangetable list to handle Vars */
    rte.r#type = NodeTag::T_RangeTblEntry;
    rte.rtekind = RTE_RELATION;
    rte.relid = relId;
    rte.relkind = RELKIND_RELATION; /* no need for exactness here */
    rte.rellockmode = AccessShareLock;

    context.rtables = list_make1!(list_make1!(&mut rte as *mut RangeTblEntry));

    /* Scan the expression tree for referenceable objects */
    find_expr_references_walker(expr, &mut context);

    /* Remove any duplicates */
    eliminate_duplicate_dependencies(context.addrs);

    /* Separate self-dependencies if necessary */
    if (behavior != self_behavior || reverse_self) && (*context.addrs).numrefs > 0 {
        let self_addrs: *mut ObjectAddresses;
        let mut outobj: *mut ObjectAddress;
        let mut oldref: c_int;
        let mut outrefs: c_int;

        self_addrs = new_object_addresses();

        outobj = (*context.addrs).refs;
        outrefs = 0;
        oldref = 0;
        while oldref < (*context.addrs).numrefs {
            let thisobj: *mut ObjectAddress = (*context.addrs).refs.add(oldref as usize);

            if (*thisobj).classId == RelationRelationId && (*thisobj).objectId == relId {
                /* Move this ref into self_addrs */
                add_exact_object_address(thisobj, self_addrs);
            } else {
                /* Keep it in context.addrs */
                *outobj = *thisobj;
                outobj = outobj.add(1);
                outrefs += 1;
            }
            oldref += 1;
        }
        (*context.addrs).numrefs = outrefs;

        /* Record the self-dependencies with the appropriate direction */
        if !reverse_self {
            recordMultipleDependencies(
                depender,
                (*self_addrs).refs,
                (*self_addrs).numrefs,
                self_behavior,
            );
        } else {
            /* Can't use recordMultipleDependencies, so do it the hard way */
            let mut selfref: c_int = 0;

            while selfref < (*self_addrs).numrefs {
                let thisobj: *mut ObjectAddress = (*self_addrs).refs.add(selfref as usize);

                recordDependencyOn(thisobj, depender, self_behavior);
                selfref += 1;
            }
        }

        free_object_addresses(self_addrs);
    }

    /* Record the external dependencies */
    recordMultipleDependencies(
        depender,
        (*context.addrs).refs,
        (*context.addrs).numrefs,
        behavior,
    );

    free_object_addresses(context.addrs);
}

/*
 * Recursively search an expression tree for object references.
 *
 * Note: in many cases we do not need to create dependencies on the datatypes
 * involved in an expression, because we'll have an indirect dependency via
 * some other object.
 */
unsafe fn find_expr_references_walker(
    node: *mut Node,
    context: *mut find_expr_references_context,
) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Var) {
        let var: *mut Var = node as *mut Var;
        let rtable: *mut List;
        let rte: *mut RangeTblEntry;

        /* Find matching rtable entry, or complain if not found */
        if (*var).varlevelsup >= list_length((*context).rtables) as u32 {
            elog!(ERROR, "invalid varlevelsup {}", (*var).varlevelsup);
        }
        rtable = list_nth((*context).rtables, (*var).varlevelsup as c_int) as *mut List;
        if (*var).varno <= 0 || (*var).varno > list_length(rtable) {
            elog!(ERROR, "invalid varno {}", (*var).varno);
        }
        rte = rt_fetch((*var).varno, rtable);

        /*
         * A whole-row Var references no specific columns, so adds no new
         * dependency.
         */
        if (*var).varattno == InvalidAttrNumber {
            return false;
        }
        if (*rte).rtekind == RTE_RELATION {
            /* If it's a plain relation, reference this column */
            add_object_address(
                RelationRelationId,
                (*rte).relid,
                (*var).varattno as int32,
                (*context).addrs,
            );
        } else if (*rte).rtekind == RTE_FUNCTION {
            /* Might need to add a dependency on a composite type's column */
            process_function_rte_ref(rte, (*var).varattno, context);
        }

        /*
         * Vars referencing other RTE types require no additional work.
         */
        return false;
    } else if IsA!(node, T_Const) {
        let con: *mut Const = node as *mut Const;
        let objoid: Oid;

        /* A constant must depend on the constant's datatype */
        add_object_address(TypeRelationId, (*con).consttype, 0, (*context).addrs);

        /*
         * We must also depend on the constant's collation.
         */
        if OidIsValid((*con).constcollid) && (*con).constcollid != DEFAULT_COLLATION_OID {
            add_object_address(
                CollationRelationId,
                (*con).constcollid,
                0,
                (*context).addrs,
            );
        }

        /*
         * If it's a regclass or similar literal referring to an existing
         * object, add a reference to that object.
         */
        if !(*con).constisnull {
            match (*con).consttype {
                t if t == REGPROCOID || t == REGPROCEDUREOID => {
                    objoid = DatumGetObjectId((*con).constvalue);
                    if SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(objoid)) {
                        add_object_address(ProcedureRelationId, objoid, 0, (*context).addrs);
                    }
                }
                t if t == REGOPEROID || t == REGOPERATOROID => {
                    objoid = DatumGetObjectId((*con).constvalue);
                    if SearchSysCacheExists1(OPEROID, ObjectIdGetDatum(objoid)) {
                        add_object_address(OperatorRelationId, objoid, 0, (*context).addrs);
                    }
                }
                t if t == REGCLASSOID => {
                    objoid = DatumGetObjectId((*con).constvalue);
                    if SearchSysCacheExists1(RELOID, ObjectIdGetDatum(objoid)) {
                        add_object_address(RelationRelationId, objoid, 0, (*context).addrs);
                    }
                }
                t if t == REGTYPEOID => {
                    objoid = DatumGetObjectId((*con).constvalue);
                    if SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(objoid)) {
                        add_object_address(TypeRelationId, objoid, 0, (*context).addrs);
                    }
                }
                t if t == REGCOLLATIONOID => {
                    objoid = DatumGetObjectId((*con).constvalue);
                    if SearchSysCacheExists1(COLLOID, ObjectIdGetDatum(objoid)) {
                        add_object_address(CollationRelationId, objoid, 0, (*context).addrs);
                    }
                }
                t if t == REGCONFIGOID => {
                    objoid = DatumGetObjectId((*con).constvalue);
                    if SearchSysCacheExists1(TSCONFIGOID, ObjectIdGetDatum(objoid)) {
                        add_object_address(TSConfigRelationId, objoid, 0, (*context).addrs);
                    }
                }
                t if t == REGDICTIONARYOID => {
                    objoid = DatumGetObjectId((*con).constvalue);
                    if SearchSysCacheExists1(TSDICTOID, ObjectIdGetDatum(objoid)) {
                        add_object_address(TSDictionaryRelationId, objoid, 0, (*context).addrs);
                    }
                }
                t if t == REGNAMESPACEOID => {
                    objoid = DatumGetObjectId((*con).constvalue);
                    if SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(objoid)) {
                        add_object_address(NamespaceRelationId, objoid, 0, (*context).addrs);
                    }
                }
                /*
                 * Dependencies for regrole should be shared among all
                 * databases, so explicitly inhibit to have dependencies.
                 */
                t if t == REGROLEOID => {
                    ereport!(
                        ERROR,
                        errmsg!("constant of the type {} cannot be used here", "regrole")
                    );
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                }
                _ => {}
            }
        }
        return false;
    } else if IsA!(node, T_Param) {
        let param: *mut Param = node as *mut Param;

        /* A parameter must depend on the parameter's datatype */
        add_object_address(TypeRelationId, (*param).paramtype, 0, (*context).addrs);
        /* and its collation, just as for Consts */
        if OidIsValid((*param).paramcollid) && (*param).paramcollid != DEFAULT_COLLATION_OID {
            add_object_address(
                CollationRelationId,
                (*param).paramcollid,
                0,
                (*context).addrs,
            );
        }
    } else if IsA!(node, T_FuncExpr) {
        let funcexpr: *mut FuncExpr = node as *mut FuncExpr;

        add_object_address(ProcedureRelationId, (*funcexpr).funcid, 0, (*context).addrs);
        /* fall through to examine arguments */
    } else if IsA!(node, T_OpExpr) {
        let opexpr: *mut OpExpr = node as *mut OpExpr;

        add_object_address(OperatorRelationId, (*opexpr).opno, 0, (*context).addrs);
        /* fall through to examine arguments */
    } else if IsA!(node, T_DistinctExpr) {
        let distinctexpr: *mut DistinctExpr = node as *mut DistinctExpr;

        add_object_address(
            OperatorRelationId,
            (*distinctexpr).opno,
            0,
            (*context).addrs,
        );
        /* fall through to examine arguments */
    } else if IsA!(node, T_NullIfExpr) {
        let nullifexpr: *mut NullIfExpr = node as *mut NullIfExpr;

        add_object_address(OperatorRelationId, (*nullifexpr).opno, 0, (*context).addrs);
        /* fall through to examine arguments */
    } else if IsA!(node, T_ScalarArrayOpExpr) {
        let opexpr: *mut ScalarArrayOpExpr = node as *mut ScalarArrayOpExpr;

        add_object_address(OperatorRelationId, (*opexpr).opno, 0, (*context).addrs);
        /* fall through to examine arguments */
    } else if IsA!(node, T_Aggref) {
        let aggref: *mut Aggref = node as *mut Aggref;

        add_object_address(ProcedureRelationId, (*aggref).aggfnoid, 0, (*context).addrs);
        /* fall through to examine arguments */
    } else if IsA!(node, T_WindowFunc) {
        let wfunc: *mut WindowFunc = node as *mut WindowFunc;

        add_object_address(ProcedureRelationId, (*wfunc).winfnoid, 0, (*context).addrs);
        /* fall through to examine arguments */
    } else if IsA!(node, T_SubscriptingRef) {
        let sbsref: *mut SubscriptingRef = node as *mut SubscriptingRef;

        /*
         * The refexpr should provide adequate dependency on refcontainertype.
         */
        if (*sbsref).refrestype != (*sbsref).refcontainertype
            && (*sbsref).refrestype != (*sbsref).refelemtype
        {
            add_object_address(TypeRelationId, (*sbsref).refrestype, 0, (*context).addrs);
        }
        /* fall through to examine arguments */
    } else if IsA!(node, T_SubPlan) {
        /* Extra work needed here if we ever need this case */
        elog!(ERROR, "already-planned subqueries not supported");
    } else if IsA!(node, T_FieldSelect) {
        let fselect: *mut FieldSelect = node as *mut FieldSelect;
        let argtype: Oid = getBaseType(exprType((*fselect).arg as *const Node));
        let reltype: Oid = get_typ_typrelid(argtype);

        /*
         * We need a dependency on the specific column named in FieldSelect,
         * assuming we can identify the pg_class OID for it.
         */
        if OidIsValid(reltype) {
            add_object_address(
                RelationRelationId,
                reltype,
                (*fselect).fieldnum as int32,
                (*context).addrs,
            );
        } else {
            add_object_address(TypeRelationId, (*fselect).resulttype, 0, (*context).addrs);
        }
        /* the collation might not be referenced anywhere else, either */
        if OidIsValid((*fselect).resultcollid)
            && (*fselect).resultcollid != DEFAULT_COLLATION_OID
        {
            add_object_address(
                CollationRelationId,
                (*fselect).resultcollid,
                0,
                (*context).addrs,
            );
        }
    } else if IsA!(node, T_FieldStore) {
        let fstore: *mut FieldStore = node as *mut FieldStore;
        let reltype: Oid = get_typ_typrelid((*fstore).resulttype);

        /* similar considerations to FieldSelect, but multiple column(s) */
        if OidIsValid(reltype) {
            foreach!(l, (*fstore).fieldnums, {
                add_object_address(
                    RelationRelationId,
                    reltype,
                    lfirst_int(current_cell!(l)),
                    (*context).addrs,
                );
            });
        } else {
            add_object_address(TypeRelationId, (*fstore).resulttype, 0, (*context).addrs);
        }
    } else if IsA!(node, T_RelabelType) {
        let relab: *mut RelabelType = node as *mut RelabelType;

        /* since there is no function dependency, need to depend on type */
        add_object_address(TypeRelationId, (*relab).resulttype, 0, (*context).addrs);
        /* the collation might not be referenced anywhere else, either */
        if OidIsValid((*relab).resultcollid) && (*relab).resultcollid != DEFAULT_COLLATION_OID {
            add_object_address(
                CollationRelationId,
                (*relab).resultcollid,
                0,
                (*context).addrs,
            );
        }
    } else if IsA!(node, T_CoerceViaIO) {
        let iocoerce: *mut CoerceViaIO = node as *mut CoerceViaIO;

        /* since there is no exposed function, need to depend on type */
        add_object_address(TypeRelationId, (*iocoerce).resulttype, 0, (*context).addrs);
        /* the collation might not be referenced anywhere else, either */
        if OidIsValid((*iocoerce).resultcollid)
            && (*iocoerce).resultcollid != DEFAULT_COLLATION_OID
        {
            add_object_address(
                CollationRelationId,
                (*iocoerce).resultcollid,
                0,
                (*context).addrs,
            );
        }
    } else if IsA!(node, T_ArrayCoerceExpr) {
        let acoerce: *mut ArrayCoerceExpr = node as *mut ArrayCoerceExpr;

        /* as above, depend on type */
        add_object_address(TypeRelationId, (*acoerce).resulttype, 0, (*context).addrs);
        /* the collation might not be referenced anywhere else, either */
        if OidIsValid((*acoerce).resultcollid)
            && (*acoerce).resultcollid != DEFAULT_COLLATION_OID
        {
            add_object_address(
                CollationRelationId,
                (*acoerce).resultcollid,
                0,
                (*context).addrs,
            );
        }
        /* fall through to examine arguments */
    } else if IsA!(node, T_ConvertRowtypeExpr) {
        let cvt: *mut ConvertRowtypeExpr = node as *mut ConvertRowtypeExpr;

        /* since there is no function dependency, need to depend on type */
        add_object_address(TypeRelationId, (*cvt).resulttype, 0, (*context).addrs);
    } else if IsA!(node, T_CollateExpr) {
        let coll: *mut CollateExpr = node as *mut CollateExpr;

        add_object_address(CollationRelationId, (*coll).collOid, 0, (*context).addrs);
    } else if IsA!(node, T_RowExpr) {
        let rowexpr: *mut RowExpr = node as *mut RowExpr;

        add_object_address(TypeRelationId, (*rowexpr).row_typeid, 0, (*context).addrs);
    } else if IsA!(node, T_RowCompareExpr) {
        let rcexpr: *mut RowCompareExpr = node as *mut RowCompareExpr;

        foreach!(l, (*rcexpr).opnos, {
            add_object_address(
                OperatorRelationId,
                lfirst_oid(current_cell!(l)),
                0,
                (*context).addrs,
            );
        });
        foreach!(l, (*rcexpr).opfamilies, {
            add_object_address(
                OperatorFamilyRelationId,
                lfirst_oid(current_cell!(l)),
                0,
                (*context).addrs,
            );
        });
        /* fall through to examine arguments */
    } else if IsA!(node, T_CoerceToDomain) {
        let cd: *mut CoerceToDomain = node as *mut CoerceToDomain;

        add_object_address(TypeRelationId, (*cd).resulttype, 0, (*context).addrs);
    } else if IsA!(node, T_NextValueExpr) {
        let nve: *mut NextValueExpr = node as *mut NextValueExpr;

        add_object_address(RelationRelationId, (*nve).seqid, 0, (*context).addrs);
    } else if IsA!(node, T_OnConflictExpr) {
        let onconflict: *mut OnConflictExpr = node as *mut OnConflictExpr;

        if OidIsValid((*onconflict).constraint) {
            add_object_address(
                ConstraintRelationId,
                (*onconflict).constraint,
                0,
                (*context).addrs,
            );
        }
        /* fall through to examine arguments */
    } else if IsA!(node, T_SortGroupClause) {
        let sgc: *mut SortGroupClause = node as *mut SortGroupClause;

        add_object_address(OperatorRelationId, (*sgc).eqop, 0, (*context).addrs);
        if OidIsValid((*sgc).sortop) {
            add_object_address(OperatorRelationId, (*sgc).sortop, 0, (*context).addrs);
        }
        return false;
    } else if IsA!(node, T_WindowClause) {
        let wc: *mut WindowClause = node as *mut WindowClause;

        if OidIsValid((*wc).startInRangeFunc) {
            add_object_address(
                ProcedureRelationId,
                (*wc).startInRangeFunc,
                0,
                (*context).addrs,
            );
        }
        if OidIsValid((*wc).endInRangeFunc) {
            add_object_address(
                ProcedureRelationId,
                (*wc).endInRangeFunc,
                0,
                (*context).addrs,
            );
        }
        if OidIsValid((*wc).inRangeColl) && (*wc).inRangeColl != DEFAULT_COLLATION_OID {
            add_object_address(
                CollationRelationId,
                (*wc).inRangeColl,
                0,
                (*context).addrs,
            );
        }
        /* fall through to examine substructure */
    } else if IsA!(node, T_CTECycleClause) {
        let cc: *mut CTECycleClause = node as *mut CTECycleClause;

        if OidIsValid((*cc).cycle_mark_type) {
            add_object_address(TypeRelationId, (*cc).cycle_mark_type, 0, (*context).addrs);
        }
        if OidIsValid((*cc).cycle_mark_collation) {
            add_object_address(
                CollationRelationId,
                (*cc).cycle_mark_collation,
                0,
                (*context).addrs,
            );
        }
        if OidIsValid((*cc).cycle_mark_neop) {
            add_object_address(
                OperatorRelationId,
                (*cc).cycle_mark_neop,
                0,
                (*context).addrs,
            );
        }
        /* fall through to examine substructure */
    } else if IsA!(node, T_Query) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        let query: *mut Query = node as *mut Query;
        let result: bool;

        /*
         * Add whole-relation refs for each plain relation mentioned in the
         * subquery's rtable, and ensure we add refs for any type-coercion
         * functions used in join alias lists.
         */
        foreach!(lc, (*query).rtable, {
            let rte: *mut RangeTblEntry = lfirst(current_cell!(lc)) as *mut RangeTblEntry;

            match (*rte).rtekind {
                RTE_RELATION => {
                    add_object_address(RelationRelationId, (*rte).relid, 0, (*context).addrs);
                }
                RTE_JOIN => {
                    /*
                     * Examine joinaliasvars entries only for merged JOIN
                     * USING columns.
                     */
                    (*context).rtables = lcons((*query).rtable as *mut c_void, (*context).rtables);
                    let mut i: c_int = 0;
                    while i < (*rte).joinmergedcols {
                        let aliasvar: *mut Node =
                            list_nth((*rte).joinaliasvars, i) as *mut Node;

                        if !IsA!(aliasvar, T_Var) {
                            find_expr_references_walker(aliasvar, context);
                        }
                        i += 1;
                    }
                    (*context).rtables = list_delete_first((*context).rtables);
                }
                RTE_NAMEDTUPLESTORE => {
                    /*
                     * Cataloged objects cannot depend on tuplestores.
                     */
                    ereport!(
                        ERROR,
                        errmsg!(
                            "transition table \"{}\" cannot be referenced in a persistent object",
                            CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy()
                        )
                    );
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                }
                _ => {
                    /* Other RTE types can be ignored here */
                }
            }
        });

        /*
         * If the query is an INSERT or UPDATE, we should create a dependency
         * on each target column.
         */
        if (*query).commandType == CMD_INSERT || (*query).commandType == CMD_UPDATE {
            let rte: *mut RangeTblEntry;

            if (*query).resultRelation <= 0
                || (*query).resultRelation > list_length((*query).rtable)
            {
                elog!(ERROR, "invalid resultRelation {}", (*query).resultRelation);
            }
            rte = rt_fetch((*query).resultRelation, (*query).rtable);
            if (*rte).rtekind == RTE_RELATION {
                foreach!(lc, (*query).targetList, {
                    let tle: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;

                    if (*tle).resjunk {
                        /* ignore junk tlist items */
                    } else {
                        add_object_address(
                            RelationRelationId,
                            (*rte).relid,
                            (*tle).resno as int32,
                            (*context).addrs,
                        );
                    }
                });
            }
        }

        /*
         * Add dependencies on constraints listed in query's constraintDeps
         */
        foreach!(lc, (*query).constraintDeps, {
            add_object_address(
                ConstraintRelationId,
                lfirst_oid(current_cell!(lc)),
                0,
                (*context).addrs,
            );
        });

        /* Examine substructure of query */
        (*context).rtables = lcons((*query).rtable as *mut c_void, (*context).rtables);
        result = query_tree_walker(
            query,
            find_expr_references_walker,
            context,
            QTW_IGNORE_JOINALIASES | QTW_EXAMINE_SORTGROUP,
        );
        (*context).rtables = list_delete_first((*context).rtables);
        return result;
    } else if IsA!(node, T_SetOperationStmt) {
        let setop: *mut SetOperationStmt = node as *mut SetOperationStmt;

        /* we need to look at the groupClauses for operator references */
        find_expr_references_walker((*setop).groupClauses as *mut Node, context);
        /* fall through to examine child nodes */
    } else if IsA!(node, T_RangeTblFunction) {
        let rtfunc: *mut RangeTblFunction = node as *mut RangeTblFunction;

        /*
         * Add refs for any datatypes and collations used in a column
         * definition list for a RECORD function.
         */
        foreach!(ct, (*rtfunc).funccoltypes, {
            add_object_address(
                TypeRelationId,
                lfirst_oid(current_cell!(ct)),
                0,
                (*context).addrs,
            );
        });
        foreach!(ct, (*rtfunc).funccolcollations, {
            let collid: Oid = lfirst_oid(current_cell!(ct));

            if OidIsValid(collid) && collid != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, collid, 0, (*context).addrs);
            }
        });
    } else if IsA!(node, T_TableFunc) {
        let tf: *mut TableFunc = node as *mut TableFunc;

        /*
         * Add refs for the datatypes and collations used in the TableFunc.
         */
        foreach!(ct, (*tf).coltypes, {
            add_object_address(
                TypeRelationId,
                lfirst_oid(current_cell!(ct)),
                0,
                (*context).addrs,
            );
        });
        foreach!(ct, (*tf).colcollations, {
            let collid: Oid = lfirst_oid(current_cell!(ct));

            if OidIsValid(collid) && collid != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, collid, 0, (*context).addrs);
            }
        });
    } else if IsA!(node, T_TableSampleClause) {
        let tsc: *mut TableSampleClause = node as *mut TableSampleClause;

        add_object_address(ProcedureRelationId, (*tsc).tsmhandler, 0, (*context).addrs);
        /* fall through to examine arguments */
    }

    expression_tree_walker(node, find_expr_references_walker, context)
}

/*
 * find_expr_references_walker subroutine: handle a Var reference
 * to an RTE_FUNCTION RTE
 */
unsafe fn process_function_rte_ref(
    rte: *mut RangeTblEntry,
    attnum: AttrNumber,
    context: *mut find_expr_references_context,
) {
    let mut atts_done: c_int = 0;

    /*
     * Identify which RangeTblFunction produces this attnum, and see if it
     * returns a composite type.
     */
    foreach!(lc, (*rte).functions, {
        let rtfunc: *mut RangeTblFunction = lfirst(current_cell!(lc)) as *mut RangeTblFunction;

        if attnum as c_int > atts_done
            && attnum as c_int <= atts_done + (*rtfunc).funccolcount
        {
            let tupdesc: crate::access::common::tupdesc::TupleDesc;

            /* If it has a coldeflist, it certainly returns RECORD */
            if !(*rtfunc).funccolnames.is_null() {
                tupdesc = null_mut(); /* no need to work hard */
            } else {
                tupdesc = get_expr_result_tupdesc((*rtfunc).funcexpr, true);
            }
            if !tupdesc.is_null() && (*tupdesc).tdtypeid != RECORDOID {
                /*
                 * Named composite type, so individual columns could get
                 * dropped.  Make a dependency on this specific column.
                 */
                let reltype: Oid = get_typ_typrelid((*tupdesc).tdtypeid);

                Assert!(attnum as c_int - atts_done <= (*tupdesc).natts);
                if OidIsValid(reltype) {
                    /* can this fail? */
                    add_object_address(
                        RelationRelationId,
                        reltype,
                        attnum as int32 - atts_done,
                        (*context).addrs,
                    );
                }
                return;
            }
            /* Nothing to do; function's result type is handled elsewhere */
            return;
        }
        atts_done += (*rtfunc).funccolcount;
    });

    /* If we get here, must be looking for the ordinality column */
    if (*rte).funcordinality && attnum as c_int == atts_done + 1 {
        return;
    }

    /* this probably can't happen ... */
    ereport!(
        ERROR,
        errmsg!(
            "column {} of relation \"{}\" does not exist",
            attnum,
            CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy()
        )
    );
    /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
}

/*
 * Given an array of dependency references, eliminate any duplicates.
 */
unsafe fn eliminate_duplicate_dependencies(addrs: *mut ObjectAddresses) {
    let mut priorobj: *mut ObjectAddress;
    let mut oldref: c_int;
    let mut newrefs: c_int;

    /*
     * We can't sort if the array has "extra" data, because there's no way to
     * keep it in sync.
     */
    Assert!((*addrs).extras.is_null());

    if (*addrs).numrefs <= 1 {
        return; /* nothing to do */
    }

    /* Sort the refs so that duplicates are adjacent */
    qsort(
        (*addrs).refs as *mut c_void,
        (*addrs).numrefs as usize,
        core::mem::size_of::<ObjectAddress>(),
        object_address_comparator,
    );

    /* Remove dups */
    priorobj = (*addrs).refs;
    newrefs = 1;
    oldref = 1;
    while oldref < (*addrs).numrefs {
        let thisobj: *mut ObjectAddress = (*addrs).refs.add(oldref as usize);

        if (*priorobj).classId == (*thisobj).classId
            && (*priorobj).objectId == (*thisobj).objectId
        {
            if (*priorobj).objectSubId == (*thisobj).objectSubId {
                /* identical, so drop thisobj */
                oldref += 1;
                continue;
            }

            /*
             * If we have a whole-object reference and a reference to a part
             * of the same object, we don't need the whole-object reference.
             */
            if (*priorobj).objectSubId == 0 {
                /* replace whole ref with partial */
                (*priorobj).objectSubId = (*thisobj).objectSubId;
                oldref += 1;
                continue;
            }
        }
        /* Not identical, so add thisobj to output set */
        priorobj = priorobj.add(1);
        *priorobj = *thisobj;
        newrefs += 1;
        oldref += 1;
    }

    (*addrs).numrefs = newrefs;
}

/*
 * qsort comparator for ObjectAddress items
 */
unsafe extern "C" fn object_address_comparator(a: *const c_void, b: *const c_void) -> c_int {
    let obja: *const ObjectAddress = a as *const ObjectAddress;
    let objb: *const ObjectAddress = b as *const ObjectAddress;

    /*
     * Primary sort key is OID descending.
     */
    if (*obja).objectId > (*objb).objectId {
        return -1;
    }
    if (*obja).objectId < (*objb).objectId {
        return 1;
    }

    /*
     * Next sort on catalog ID, in case identical OIDs appear in different
     * catalogs.
     */
    if (*obja).classId < (*objb).classId {
        return -1;
    }
    if (*obja).classId > (*objb).classId {
        return 1;
    }

    /*
     * Last, sort on object subId.
     *
     * We sort the subId as an unsigned int so that 0 (the whole object) will
     * come first.
     */
    if ((*obja).objectSubId as u32) < ((*objb).objectSubId as u32) {
        return -1;
    }
    if ((*obja).objectSubId as u32) > ((*objb).objectSubId as u32) {
        return 1;
    }
    0
}

/*
 * Routines for handling an expansible array of ObjectAddress items.
 *
 * new_object_addresses: create a new ObjectAddresses array.
 */
pub unsafe fn new_object_addresses() -> *mut ObjectAddresses {
    let addrs: *mut ObjectAddresses;

    addrs = palloc(core::mem::size_of::<ObjectAddresses>()) as *mut ObjectAddresses;

    (*addrs).numrefs = 0;
    (*addrs).maxrefs = 32;
    (*addrs).refs =
        palloc((*addrs).maxrefs as usize * core::mem::size_of::<ObjectAddress>())
            as *mut ObjectAddress;
    (*addrs).extras = null_mut(); /* until/unless needed */

    addrs
}

/*
 * Add an entry to an ObjectAddresses array.
 */
unsafe fn add_object_address(
    classId: Oid,
    objectId: Oid,
    subId: int32,
    addrs: *mut ObjectAddresses,
) {
    let item: *mut ObjectAddress;

    /* enlarge array if needed */
    if (*addrs).numrefs >= (*addrs).maxrefs {
        (*addrs).maxrefs *= 2;
        (*addrs).refs = repalloc(
            (*addrs).refs as *mut c_void,
            (*addrs).maxrefs as usize * core::mem::size_of::<ObjectAddress>(),
        ) as *mut ObjectAddress;
        Assert!((*addrs).extras.is_null());
    }
    /* record this item */
    item = (*addrs).refs.add((*addrs).numrefs as usize);
    (*item).classId = classId;
    (*item).objectId = objectId;
    (*item).objectSubId = subId;
    (*addrs).numrefs += 1;
}

/*
 * Add an entry to an ObjectAddresses array.
 *
 * As above, but specify entry exactly.
 */
pub unsafe fn add_exact_object_address(
    object: *const ObjectAddress,
    addrs: *mut ObjectAddresses,
) {
    let item: *mut ObjectAddress;

    /* enlarge array if needed */
    if (*addrs).numrefs >= (*addrs).maxrefs {
        (*addrs).maxrefs *= 2;
        (*addrs).refs = repalloc(
            (*addrs).refs as *mut c_void,
            (*addrs).maxrefs as usize * core::mem::size_of::<ObjectAddress>(),
        ) as *mut ObjectAddress;
        Assert!((*addrs).extras.is_null());
    }
    /* record this item */
    item = (*addrs).refs.add((*addrs).numrefs as usize);
    *item = *object;
    (*addrs).numrefs += 1;
}

/*
 * Add an entry to an ObjectAddresses array.
 *
 * As above, but specify entry exactly and provide some "extra" data too.
 */
unsafe fn add_exact_object_address_extra(
    object: *const ObjectAddress,
    extra: *const ObjectAddressExtra,
    addrs: *mut ObjectAddresses,
) {
    let item: *mut ObjectAddress;
    let itemextra: *mut ObjectAddressExtra;

    /* allocate extra space if first time */
    if (*addrs).extras.is_null() {
        (*addrs).extras = palloc(
            (*addrs).maxrefs as usize * core::mem::size_of::<ObjectAddressExtra>(),
        ) as *mut ObjectAddressExtra;
    }

    /* enlarge array if needed */
    if (*addrs).numrefs >= (*addrs).maxrefs {
        (*addrs).maxrefs *= 2;
        (*addrs).refs = repalloc(
            (*addrs).refs as *mut c_void,
            (*addrs).maxrefs as usize * core::mem::size_of::<ObjectAddress>(),
        ) as *mut ObjectAddress;
        (*addrs).extras = repalloc(
            (*addrs).extras as *mut c_void,
            (*addrs).maxrefs as usize * core::mem::size_of::<ObjectAddressExtra>(),
        ) as *mut ObjectAddressExtra;
    }
    /* record this item */
    item = (*addrs).refs.add((*addrs).numrefs as usize);
    *item = *object;
    itemextra = (*addrs).extras.add((*addrs).numrefs as usize);
    *itemextra = *extra;
    (*addrs).numrefs += 1;
}

/*
 * Test whether an object is present in an ObjectAddresses array.
 *
 * We return "true" if object is a subobject of something in the array, too.
 */
pub unsafe fn object_address_present(
    object: *const ObjectAddress,
    addrs: *const ObjectAddresses,
) -> bool {
    let mut i: c_int;

    i = (*addrs).numrefs - 1;
    while i >= 0 {
        let thisobj: *const ObjectAddress = (*addrs).refs.add(i as usize);

        if (*object).classId == (*thisobj).classId
            && (*object).objectId == (*thisobj).objectId
        {
            if (*object).objectSubId == (*thisobj).objectSubId || (*thisobj).objectSubId == 0 {
                return true;
            }
        }
        i -= 1;
    }

    false
}

/*
 * As above, except that if the object is present then also OR the given
 * flags into its associated extra data (which must exist).
 */
unsafe fn object_address_present_add_flags(
    object: *const ObjectAddress,
    flags: c_int,
    addrs: *mut ObjectAddresses,
) -> bool {
    let mut result: bool = false;
    let mut i: c_int;

    i = (*addrs).numrefs - 1;
    while i >= 0 {
        let thisobj: *mut ObjectAddress = (*addrs).refs.add(i as usize);

        if (*object).classId == (*thisobj).classId
            && (*object).objectId == (*thisobj).objectId
        {
            if (*object).objectSubId == (*thisobj).objectSubId {
                let thisextra: *mut ObjectAddressExtra = (*addrs).extras.add(i as usize);

                (*thisextra).flags |= flags;
                result = true;
            } else if (*thisobj).objectSubId == 0 {
                /*
                 * We get here if we find a need to delete a column after
                 * having already decided to drop its whole table.
                 */
                result = true;
            } else if (*object).objectSubId == 0 {
                /*
                 * We get here if we find a need to delete a whole table after
                 * having already decided to drop one of its columns.
                 */
                let thisextra: *mut ObjectAddressExtra = (*addrs).extras.add(i as usize);

                if flags != 0 {
                    (*thisextra).flags |= flags | DEPFLAG_SUBOBJECT;
                }
            }
        }
        i -= 1;
    }

    result
}

/*
 * Similar to above, except we search an ObjectAddressStack.
 */
unsafe fn stack_address_present_add_flags(
    object: *const ObjectAddress,
    flags: c_int,
    stack: *mut ObjectAddressStack,
) -> bool {
    let mut result: bool = false;
    let mut stackptr: *mut ObjectAddressStack;

    stackptr = stack;
    while !stackptr.is_null() {
        let thisobj: *const ObjectAddress = (*stackptr).object;

        if (*object).classId == (*thisobj).classId
            && (*object).objectId == (*thisobj).objectId
        {
            if (*object).objectSubId == (*thisobj).objectSubId {
                (*stackptr).flags |= flags;
                result = true;
            } else if (*thisobj).objectSubId == 0 {
                /*
                 * We're visiting a column with whole table already on stack.
                 */
                result = true;
            } else if (*object).objectSubId == 0 {
                /*
                 * We're visiting a table with column already on stack.
                 */
                if flags != 0 {
                    (*stackptr).flags |= flags | DEPFLAG_SUBOBJECT;
                }
            }
        }
        stackptr = (*stackptr).next;
    }

    result
}

/*
 * Record multiple dependencies from an ObjectAddresses array, after first
 * removing any duplicates.
 */
pub unsafe fn record_object_address_dependencies(
    depender: *const ObjectAddress,
    referenced: *mut ObjectAddresses,
    behavior: DependencyType,
) {
    eliminate_duplicate_dependencies(referenced);
    recordMultipleDependencies(
        depender,
        (*referenced).refs,
        (*referenced).numrefs,
        behavior,
    );
}

/*
 * Sort the items in an ObjectAddresses array.
 */
pub unsafe fn sort_object_addresses(addrs: *mut ObjectAddresses) {
    if (*addrs).numrefs > 1 {
        qsort(
            (*addrs).refs as *mut c_void,
            (*addrs).numrefs as usize,
            core::mem::size_of::<ObjectAddress>(),
            object_address_comparator,
        );
    }
}

/*
 * Clean up when done with an ObjectAddresses array.
 */
pub unsafe fn free_object_addresses(addrs: *mut ObjectAddresses) {
    pfree((*addrs).refs as *mut c_void);
    if !(*addrs).extras.is_null() {
        pfree((*addrs).extras as *mut c_void);
    }
    pfree(addrs as *mut c_void);
}

/*
 * delete initial ACL for extension objects
 */
unsafe fn DeleteInitPrivs(object: *const ObjectAddress) {
    let relation: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let nkeys: c_int;
    let scan: SysScanDesc;
    let mut oldtuple: HeapTuple;

    relation = table_open(InitPrivsRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_init_privs_objoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).objectId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_init_privs_classoid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum((*object).classId),
    );
    if (*object).objectSubId != 0 {
        ScanKeyInit(
            &mut key[2],
            Anum_pg_init_privs_objsubid,
            BTEqualStrategyNumber,
            F_INT4EQ,
            Int32GetDatum((*object).objectSubId),
        );
        nkeys = 3;
    } else {
        nkeys = 2;
    }

    scan = systable_beginscan(
        relation,
        InitPrivsObjIndexId,
        true,
        null_mut(),
        nkeys,
        key.as_mut_ptr(),
    );

    loop {
        oldtuple = systable_getnext(scan) as HeapTuple;
        if !HeapTupleIsValid(oldtuple) {
            break;
        }
        CatalogTupleDelete(relation, &mut (*oldtuple).t_self);
    }

    systable_endscan(scan);

    table_close(relation, RowExclusiveLock);
}
