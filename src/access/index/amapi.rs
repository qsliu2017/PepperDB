//! Source: postgres/src/backend/access/index/amapi.c
//!
//! MERGED from postgres/src/include/access/amapi.h:
//!   - `IndexAMProperty` enum (AMPROP_*)
//!   - `OpFamilyMember` struct
//!   - the AM callback fn-pointer typedefs (am*_function)
//!   - the `IndexAmRoutine` API struct (capability flags + method pointers)
//!   - extern decls GetIndexAmRoutine / GetIndexAmRoutineByAmId /
//!     IndexAmTranslateStrategy / IndexAmTranslateCompareType
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/amapi.h"              -> this file
//!   "access/htup_details.h"       -> GETSTRUCT (STUB: syscache path)
//!   "catalog/pg_am.h"             -> crate::catalog::pg_am (Form_pg_am, AMTYPE_INDEX)
//!   "catalog/pg_opclass.h"        -> Form_pg_opclass (STUB: syscache path)
//!   "utils/fmgrprotos.h"          -> (V1 amvalidate signature)
//!   "utils/syscache.h"            -> SearchSysCache1/ReleaseSysCache (STUB)
//!   "access/cmptype.h"            -> CompareType + COMPARE_* (cmptype.h NOT ported; local consts)
//!   "access/genam.h"              -> IndexScanDesc/IndexBuildResult/... (NOT ported; opaque aliases)
//!   "access/stratnum.h"           -> crate::access::stratnum (StrategyNumber, InvalidStrategy, BTMaxStrategyNumber)

use crate::prelude::*;

use crate::access::stratnum::{BTMaxStrategyNumber, InvalidStrategy, StrategyNumber};
use crate::catalog::pg_am::{Form_pg_am, AMTYPE_INDEX};
use crate::nodes::nodes::{nodeTag, Cost, NodeTag, Selectivity};
use crate::nodes::pg_list::List;
use crate::nodes::plannodes::ScanDirection;
use crate::nodes::primnodes::CompareType;
use crate::utils::fmgr::{FunctionCallInfo, OidFunctionCall0Coll};

// ---------------------------------------------------------------------------
// cmptype.h is not yet ported. CompareType is aliased to c_int in primnodes.
// Reproduce the two values amapi.c relies on (the shortcut bounds checks).
// TODO(pg-port): replace with crate::access::cmptype once that file is translated.
// ---------------------------------------------------------------------------
pub const COMPARE_INVALID: CompareType = 0;
pub const COMPARE_LT: CompareType = 1;
pub const COMPARE_LE: CompareType = 2;
pub const COMPARE_EQ: CompareType = 3;
pub const COMPARE_GE: CompareType = 4;
pub const COMPARE_GT: CompareType = 5;

// ---------------------------------------------------------------------------
// Opaque aliases for AM-interface types that are not yet ported. These are the
// parameter/return types of the method pointers below. Where a ported type
// exists we use it; the rest are `c_void` placeholders so the fn-pointer
// signatures stay structurally faithful (a pointer is a pointer).
// ---------------------------------------------------------------------------

// Ported elsewhere (preferred over opaque aliases):
pub use crate::nodes::execnodes::Relation;
pub use crate::storage::itemptr::ItemPointer;

/// TODO(pg-port): real def `typedef struct IndexScanDescData *IndexScanDesc` in access/relscan.h.
pub type IndexScanDesc = *mut c_void;
/// TODO(pg-port): real def `struct IndexBuildResult` in access/genam.h.
pub type IndexBuildResult = c_void;
/// TODO(pg-port): real def `struct IndexBulkDeleteResult` in access/genam.h.
pub type IndexBulkDeleteResult = c_void;
/// TODO(pg-port): real def `struct IndexVacuumInfo` in access/genam.h.
pub type IndexVacuumInfo = c_void;
/// TODO(pg-port): real def `struct IndexInfo` in nodes/execnodes.h.
pub type IndexInfo = c_void;
/// TODO(pg-port): real def `struct ScanKeyData *ScanKey` in access/skey.h.
pub type ScanKey = *mut c_void;
/// TODO(pg-port): real def `struct TIDBitmap` in nodes/tidbitmap.h.
pub type TIDBitmap = c_void;
/// TODO(pg-port): real def `struct PlannerInfo` in nodes/pathnodes.h.
pub type PlannerInfo = c_void;
/// TODO(pg-port): real def `struct IndexPath` in nodes/pathnodes.h.
pub type IndexPath = c_void;
/// TODO(pg-port): real def `typedef enum IndexUniqueCheck` in access/genam.h.
pub type IndexUniqueCheck = c_int;
/// TODO(pg-port): real def in access/genam.h:
/// `typedef bool (*IndexBulkDeleteCallback) (ItemPointer itemptr, void *state);`
pub type IndexBulkDeleteCallback =
    Option<unsafe extern "C" fn(itemptr: ItemPointer, state: *mut c_void) -> bool>;

/// TODO(pg-port): real def `struct varlena` -> `bytea`. bytea IS ported in c.rs.
pub use crate::c::bytea;

// ---------------------------------------------------------------------------
// Properties for the amproperty API (amapi.h IndexAMProperty enum).
// ---------------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IndexAMProperty {
    AMPROP_UNKNOWN = 0,        /* anything not known to core code */
    AMPROP_ASC,               /* column properties */
    AMPROP_DESC,
    AMPROP_NULLS_FIRST,
    AMPROP_NULLS_LAST,
    AMPROP_ORDERABLE,
    AMPROP_DISTANCE_ORDERABLE,
    AMPROP_RETURNABLE,
    AMPROP_SEARCH_ARRAY,
    AMPROP_SEARCH_NULLS,
    AMPROP_CLUSTERABLE, /* index properties */
    AMPROP_INDEX_SCAN,
    AMPROP_BITMAP_SCAN,
    AMPROP_BACKWARD_SCAN,
    AMPROP_CAN_ORDER, /* AM properties */
    AMPROP_CAN_UNIQUE,
    AMPROP_CAN_MULTI_COL,
    AMPROP_CAN_EXCLUDE,
    AMPROP_CAN_INCLUDE,
}
pub use IndexAMProperty::*;

// ---------------------------------------------------------------------------
// OpFamilyMember (amapi.h): used while building/adding to an opclass/opfamily.
// amadjustmembers functions receive lists of these and may alter their "ref"
// fields.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct OpFamilyMember {
    pub is_func: bool,        /* is this an operator, or support func? */
    pub object: Oid,          /* operator or support func's OID */
    pub number: c_int,        /* strategy or support func number */
    pub lefttype: Oid,        /* lefttype */
    pub righttype: Oid,       /* righttype */
    pub sortfamily: Oid,      /* ordering operator's sort opfamily, or 0 */
    pub ref_is_hard: bool,    /* hard or soft dependency? */
    pub ref_is_family: bool,  /* is dependency on opclass or opfamily? */
    pub refobjid: Oid,        /* OID of opclass or opfamily */
}

// ---------------------------------------------------------------------------
// Callback function-pointer typedefs --- see indexam.sgml for more info.
// Each C `typedef RET (*name) (args)` becomes a Rust
// `pub type name = Option<unsafe extern "C" fn(args) -> RET>;`
// so a NULL function pointer is representable (many fields "can be NULL").
// ---------------------------------------------------------------------------

/* translate AM-specific strategies to general operator types */
pub type amtranslate_strategy_function =
    Option<unsafe extern "C" fn(strategy: StrategyNumber, opfamily: Oid) -> CompareType>;

/* translate general operator types to AM-specific strategies */
pub type amtranslate_cmptype_function =
    Option<unsafe extern "C" fn(cmptype: CompareType, opfamily: Oid) -> StrategyNumber>;

/* build new index */
pub type ambuild_function = Option<
    unsafe extern "C" fn(
        heapRelation: Relation,
        indexRelation: Relation,
        indexInfo: *mut IndexInfo,
    ) -> *mut IndexBuildResult,
>;

/* build empty index */
pub type ambuildempty_function = Option<unsafe extern "C" fn(indexRelation: Relation)>;

/* insert this tuple */
pub type aminsert_function = Option<
    unsafe extern "C" fn(
        indexRelation: Relation,
        values: *mut Datum,
        isnull: *mut bool,
        heap_tid: ItemPointer,
        heapRelation: Relation,
        checkUnique: IndexUniqueCheck,
        indexUnchanged: bool,
        indexInfo: *mut IndexInfo,
    ) -> bool,
>;

/* cleanup after insert */
pub type aminsertcleanup_function =
    Option<unsafe extern "C" fn(indexRelation: Relation, indexInfo: *mut IndexInfo)>;

/* bulk delete */
pub type ambulkdelete_function = Option<
    unsafe extern "C" fn(
        info: *mut IndexVacuumInfo,
        stats: *mut IndexBulkDeleteResult,
        callback: IndexBulkDeleteCallback,
        callback_state: *mut c_void,
    ) -> *mut IndexBulkDeleteResult,
>;

/* post-VACUUM cleanup */
pub type amvacuumcleanup_function = Option<
    unsafe extern "C" fn(
        info: *mut IndexVacuumInfo,
        stats: *mut IndexBulkDeleteResult,
    ) -> *mut IndexBulkDeleteResult,
>;

/* can indexscan return IndexTuples? */
pub type amcanreturn_function =
    Option<unsafe extern "C" fn(indexRelation: Relation, attno: c_int) -> bool>;

/* estimate cost of an indexscan */
pub type amcostestimate_function = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        path: *mut IndexPath,
        loop_count: f64,
        indexStartupCost: *mut Cost,
        indexTotalCost: *mut Cost,
        indexSelectivity: *mut Selectivity,
        indexCorrelation: *mut f64,
        indexPages: *mut f64,
    ),
>;

/* estimate height of a tree-structured index */
pub type amgettreeheight_function = Option<unsafe extern "C" fn(rel: Relation) -> c_int>;

/* parse index reloptions */
pub type amoptions_function =
    Option<unsafe extern "C" fn(reloptions: Datum, validate: bool) -> *mut bytea>;

/* report AM, index, or index column property */
pub type amproperty_function = Option<
    unsafe extern "C" fn(
        index_oid: Oid,
        attno: c_int,
        prop: IndexAMProperty,
        propname: *const c_char,
        res: *mut bool,
        isnull: *mut bool,
    ) -> bool,
>;

/* name of phase as used in progress reporting */
pub type ambuildphasename_function =
    Option<unsafe extern "C" fn(phasenum: int64) -> *mut c_char>;

/* validate definition of an opclass for this AM */
pub type amvalidate_function = Option<unsafe extern "C" fn(opclassoid: Oid) -> bool>;

/* validate operators and support functions to be added to an opclass/family */
pub type amadjustmembers_function = Option<
    unsafe extern "C" fn(
        opfamilyoid: Oid,
        opclassoid: Oid,
        operators: *mut List,
        functions: *mut List,
    ),
>;

/* prepare for index scan */
pub type ambeginscan_function = Option<
    unsafe extern "C" fn(indexRelation: Relation, nkeys: c_int, norderbys: c_int) -> IndexScanDesc,
>;

/* (re)start index scan */
pub type amrescan_function = Option<
    unsafe extern "C" fn(
        scan: IndexScanDesc,
        keys: ScanKey,
        nkeys: c_int,
        orderbys: ScanKey,
        norderbys: c_int,
    ),
>;

/* next valid tuple */
pub type amgettuple_function =
    Option<unsafe extern "C" fn(scan: IndexScanDesc, direction: ScanDirection) -> bool>;

/* fetch all valid tuples */
pub type amgetbitmap_function =
    Option<unsafe extern "C" fn(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64>;

/* end index scan */
pub type amendscan_function = Option<unsafe extern "C" fn(scan: IndexScanDesc)>;

/* mark current scan position */
pub type ammarkpos_function = Option<unsafe extern "C" fn(scan: IndexScanDesc)>;

/* restore marked scan position */
pub type amrestrpos_function = Option<unsafe extern "C" fn(scan: IndexScanDesc)>;

/*
 * Callback function signatures - for parallel index scans.
 */

/* estimate size of parallel scan descriptor */
pub type amestimateparallelscan_function = Option<
    unsafe extern "C" fn(indexRelation: Relation, nkeys: c_int, norderbys: c_int) -> Size,
>;

/* prepare for parallel index scan */
pub type aminitparallelscan_function = Option<unsafe extern "C" fn(target: *mut c_void)>;

/* (re)start parallel index scan */
pub type amparallelrescan_function = Option<unsafe extern "C" fn(scan: IndexScanDesc)>;

// ---------------------------------------------------------------------------
// API struct for an index AM. Note this must be stored in a single palloc'd
// chunk of memory. (amapi.h IndexAmRoutine)
//
// Every field is reproduced in source order. The capability flags are plain
// scalars; the method pointers are the Option<fn> typedefs above (NULL <-> None).
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct IndexAmRoutine {
    pub r#type: NodeTag,

    /*
     * Total number of strategies (operators) by which we can traverse/search
     * this AM. Zero if AM does not have a fixed set of strategy assignments.
     */
    pub amstrategies: uint16,
    /* total number of support functions that this AM uses */
    pub amsupport: uint16,
    /* opclass options support function number or 0 */
    pub amoptsprocnum: uint16,
    /* does AM support ORDER BY indexed column's value? */
    pub amcanorder: bool,
    /* does AM support ORDER BY result of an operator on indexed column? */
    pub amcanorderbyop: bool,
    /* does AM support hashing using API consistent with the hash AM? */
    pub amcanhash: bool,
    /* do operators within an opfamily have consistent equality semantics? */
    pub amconsistentequality: bool,
    /* do operators within an opfamily have consistent ordering semantics? */
    pub amconsistentordering: bool,
    /* does AM support backward scanning? */
    pub amcanbackward: bool,
    /* does AM support UNIQUE indexes? */
    pub amcanunique: bool,
    /* does AM support multi-column indexes? */
    pub amcanmulticol: bool,
    /* does AM require scans to have a constraint on the first index column? */
    pub amoptionalkey: bool,
    /* does AM handle ScalarArrayOpExpr quals? */
    pub amsearcharray: bool,
    /* does AM handle IS NULL/IS NOT NULL quals? */
    pub amsearchnulls: bool,
    /* can index storage data type differ from column data type? */
    pub amstorage: bool,
    /* can an index of this type be clustered on? */
    pub amclusterable: bool,
    /* does AM handle predicate locks? */
    pub ampredlocks: bool,
    /* does AM support parallel scan? */
    pub amcanparallel: bool,
    /* does AM support parallel build? */
    pub amcanbuildparallel: bool,
    /* does AM support columns included with clause INCLUDE? */
    pub amcaninclude: bool,
    /* does AM use maintenance_work_mem? */
    pub amusemaintenanceworkmem: bool,
    /* does AM store tuple information only at block granularity? */
    pub amsummarizing: bool,
    /* OR of parallel vacuum flags. See vacuum.h for flags. */
    pub amparallelvacuumoptions: uint8,
    /* type of data stored in index, or InvalidOid if variable */
    pub amkeytype: Oid,

    /*
     * If you add new properties to either the above or the below lists, then
     * they should also (usually) be exposed via the property API (see
     * IndexAMProperty at the top of the file, and utils/adt/amutils.c).
     */

    /* interface functions */
    pub ambuild: ambuild_function,
    pub ambuildempty: ambuildempty_function,
    pub aminsert: aminsert_function,
    pub aminsertcleanup: aminsertcleanup_function, /* can be NULL */
    pub ambulkdelete: ambulkdelete_function,
    pub amvacuumcleanup: amvacuumcleanup_function,
    pub amcanreturn: amcanreturn_function, /* can be NULL */
    pub amcostestimate: amcostestimate_function,
    pub amgettreeheight: amgettreeheight_function, /* can be NULL */
    pub amoptions: amoptions_function,
    pub amproperty: amproperty_function, /* can be NULL */
    pub ambuildphasename: ambuildphasename_function, /* can be NULL */
    pub amvalidate: amvalidate_function,
    pub amadjustmembers: amadjustmembers_function, /* can be NULL */
    pub ambeginscan: ambeginscan_function,
    pub amrescan: amrescan_function,
    pub amgettuple: amgettuple_function, /* can be NULL */
    pub amgetbitmap: amgetbitmap_function, /* can be NULL */
    pub amendscan: amendscan_function,
    pub ammarkpos: ammarkpos_function, /* can be NULL */
    pub amrestrpos: amrestrpos_function, /* can be NULL */

    /* interface functions to support parallel index scans */
    pub amestimateparallelscan: amestimateparallelscan_function, /* can be NULL */
    pub aminitparallelscan: aminitparallelscan_function, /* can be NULL */
    pub amparallelrescan: amparallelrescan_function, /* can be NULL */

    /* interface functions to support planning */
    pub amtranslatestrategy: amtranslate_strategy_function, /* can be NULL */
    pub amtranslatecmptype: amtranslate_cmptype_function,   /* can be NULL */
}

// ---------------------------------------------------------------------------
// NodeTag::T_IndexAmRoutine is not present in the ported NodeTag enum yet, so
// the `IsA(routine, IndexAmRoutine)` check below cannot use the IsA! macro
// (which expands to `NodeTag::T_IndexAmRoutine`). Provide a local helper that
// mirrors the macro: compare nodeTag(ptr) against the tag.
//
// TODO(pg-port): once nodes.rs gains T_IndexAmRoutine, replace this with
//   IsA!(routine, T_IndexAmRoutine).
// ---------------------------------------------------------------------------
#[inline]
unsafe fn IsA_IndexAmRoutine(routine: *const IndexAmRoutine) -> bool {
    // The handler's returned node carries type == T_IndexAmRoutine. Until that
    // variant exists we read the tag and compare via the struct's own stored
    // discriminant by round-tripping through nodeTag for shape-compatibility.
    // Since we cannot name the missing variant, accept any node whose first
    // word was set by makeNode(IndexAmRoutine); handlers always do so. This is
    // a faithfulness STUB: it does not actually validate the discriminant.
    let _ = nodeTag(routine);
    !routine.is_null()
}

// ---------------------------------------------------------------------------
// GetIndexAmRoutine - call the specified access method handler routine to get
// its IndexAmRoutine struct, which will be palloc'd in the caller's context.
//
// Note that if the amhandler function is built-in, this will not involve any
// catalog access. It's therefore safe to use this while bootstrapping indexes
// for the system catalogs. relcache.c relies on that.
// ---------------------------------------------------------------------------
pub unsafe fn GetIndexAmRoutine(amhandler: Oid) -> *mut IndexAmRoutine {
    let datum: Datum = OidFunctionCall0Coll(amhandler, InvalidOid);
    let routine = DatumGetPointer(datum) as *mut IndexAmRoutine;

    if routine.is_null() || !IsA_IndexAmRoutine(routine) {
        elog!(
            ERROR,
            "index access method handler function {} did not return an IndexAmRoutine struct",
            amhandler
        );
    }

    routine
}

// ---------------------------------------------------------------------------
// GetIndexAmRoutineByAmId - look up the handler of the index access method with
// the given OID, and get its IndexAmRoutine struct.
//
// If the given OID isn't a valid index access method, returns NULL if noerror
// is true, else throws error.
//
// STUB: the SearchSysCache1(AMOID, ...) / GETSTRUCT / ReleaseSysCache path is
// not yet wired (syscache.c unported). The structure of the original (extract
// amhandler from the pg_am tuple, validate amtype == AMTYPE_INDEX, then call
// GetIndexAmRoutine) is preserved as comments / unimplemented body.
// ---------------------------------------------------------------------------
pub unsafe fn GetIndexAmRoutineByAmId(amoid: Oid, noerror: bool) -> *mut IndexAmRoutine {
    // TODO(pg-port): real implementation needs syscache (AMOID) + GETSTRUCT.
    //
    //   let tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
    //   if (!HeapTupleIsValid(tuple)) {
    //       if noerror { return null_mut(); }
    //       elog!(ERROR, "cache lookup failed for access method {}", amoid);
    //   }
    //   let amform: Form_pg_am = GETSTRUCT(tuple) as Form_pg_am;
    //   if (*amform).amtype != AMTYPE_INDEX {
    //       if noerror { ReleaseSysCache(tuple); return null_mut(); }
    //       ereport!(ERROR, errmsg!("access method \"{}\" is not of type {}",
    //                NameStr((*amform).amname), "INDEX"));
    //   }
    //   let amhandler = (*amform).amhandler;
    //   if !RegProcedureIsValid(amhandler) {
    //       if noerror { ReleaseSysCache(tuple); return null_mut(); }
    //       ereport!(ERROR, errmsg!("index access method \"{}\" does not have a handler",
    //                NameStr((*amform).amname)));
    //   }
    //   ReleaseSysCache(tuple);
    //   return GetIndexAmRoutine(amhandler);
    let _ = (amoid, noerror, AMTYPE_INDEX);
    let _phantom: Option<Form_pg_am> = None;
    unimplemented!("GetIndexAmRoutineByAmId: requires syscache AMOID lookup (syscache.c unported)")
}

// ---------------------------------------------------------------------------
// IndexAmTranslateStrategy - given an access method and strategy, get the
// corresponding compare type.
//
// If missing_ok is false, throw an error if no compare type is found. If true,
// just return COMPARE_INVALID.
// ---------------------------------------------------------------------------
pub unsafe fn IndexAmTranslateStrategy(
    strategy: StrategyNumber,
    amoid: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> CompareType {
    use crate::catalog::pg_known_oids::BTREE_AM_OID;

    /* shortcut for common case */
    if amoid == BTREE_AM_OID && strategy > InvalidStrategy && strategy <= BTMaxStrategyNumber {
        return strategy as CompareType;
    }

    let amroutine = GetIndexAmRoutineByAmId(amoid, false);
    let result = match (*amroutine).amtranslatestrategy {
        Some(f) => f(strategy, opfamily),
        None => COMPARE_INVALID,
    };

    if !missing_ok && result == COMPARE_INVALID {
        elog!(
            ERROR,
            "could not translate strategy number {} for index AM {}",
            strategy,
            amoid
        );
    }

    result
}

// ---------------------------------------------------------------------------
// IndexAmTranslateCompareType - given an access method and compare type, get the
// corresponding strategy number.
//
// If missing_ok is false, throw an error if no strategy is found correlating to
// the given cmptype. If true, just return InvalidStrategy.
// ---------------------------------------------------------------------------
pub unsafe fn IndexAmTranslateCompareType(
    cmptype: CompareType,
    amoid: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> StrategyNumber {
    use crate::catalog::pg_known_oids::BTREE_AM_OID;

    /* shortcut for common case */
    if amoid == BTREE_AM_OID && cmptype > COMPARE_INVALID && cmptype <= COMPARE_GT {
        return cmptype as StrategyNumber;
    }

    let amroutine = GetIndexAmRoutineByAmId(amoid, false);
    let result = match (*amroutine).amtranslatecmptype {
        Some(f) => f(cmptype, opfamily),
        None => InvalidStrategy,
    };

    if !missing_ok && result == InvalidStrategy {
        elog!(
            ERROR,
            "could not translate compare type {} for index AM {}",
            cmptype,
            amoid
        );
    }

    result
}

// ---------------------------------------------------------------------------
// amvalidate(PG_FUNCTION_ARGS): ask the appropriate access method to validate
// the specified opclass.
//
// STUB: the SearchSysCache1(CLAOID, ...) / GETSTRUCT path (Form_pg_opclass ->
// opcmethod) is unported. The dispatch onto amroutine->amvalidate is preserved
// in comments.
// ---------------------------------------------------------------------------
pub unsafe fn amvalidate(fcinfo: FunctionCallInfo) -> Datum {
    // let opclassoid = PG_GETARG_OID!(fcinfo, 0);
    //
    // let classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    // if !HeapTupleIsValid(classtup) {
    //     elog!(ERROR, "cache lookup failed for operator class {}", opclassoid);
    // }
    // let classform: Form_pg_opclass = GETSTRUCT(classtup) as Form_pg_opclass;
    // let amoid = (*classform).opcmethod;
    // ReleaseSysCache(classtup);
    //
    // let amroutine = GetIndexAmRoutineByAmId(amoid, false);
    // if (*amroutine).amvalidate.is_none() {
    //     elog!(ERROR, "function amvalidate is not defined for index access method {}", amoid);
    // }
    // let result = ((*amroutine).amvalidate.unwrap())(opclassoid);
    // pfree(amroutine as *mut c_void);
    // PG_RETURN_BOOL!(result)
    let _ = fcinfo;
    unimplemented!("amvalidate: requires syscache CLAOID lookup (syscache.c / pg_opclass unported)")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an IndexAmRoutine by hand (palloc0 + set a few fields) and read a
    /// capability flag back. This exercises the struct layout and field access
    /// without needing a real handler (GetIndexAmRoutine is not tested as it
    /// requires OidFunctionCall0 of a live handler).
    #[test]
    fn build_and_read_routine() {
        unsafe {
            let routine =
                palloc0(core::mem::size_of::<IndexAmRoutine>()) as *mut IndexAmRoutine;
            assert!(!routine.is_null());

            // palloc0 zeroes everything: all Option<fn> are None, all flags false.
            (*routine).amstrategies = BTMaxStrategyNumber;
            (*routine).amsupport = 3;
            (*routine).amcanorder = true;
            (*routine).amcanmulticol = true;

            assert_eq!((*routine).amstrategies, 5);
            assert_eq!((*routine).amsupport, 3);
            assert!((*routine).amcanorder);
            assert!((*routine).amcanmulticol);
            // a method pointer left zeroed reads back as None
            assert!((*routine).ambuild.is_none());
            assert!((*routine).amgettuple.is_none());

            pfree(routine as *mut c_void);
        }
    }

    #[test]
    fn cmptype_shortcut_bounds() {
        // COMPARE_GT is the upper bound used by the IndexAmTranslateCompareType
        // shortcut; verify the local consts have the expected ordering.
        assert!(COMPARE_INVALID < COMPARE_LT);
        assert_eq!(COMPARE_GT, 5);
        assert!(COMPARE_EQ > COMPARE_INVALID && COMPARE_EQ <= COMPARE_GT);
    }

    #[test]
    fn property_enum_values() {
        assert_eq!(AMPROP_UNKNOWN as i32, 0);
        assert_eq!(AMPROP_CAN_INCLUDE as i32, 18);
    }
}
