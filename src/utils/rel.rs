//! Translation of postgres/src/include/utils/rel.h
//!   POSTGRES relation descriptor (a/k/a relcache entry) definitions.
//!
//! This is the real #[repr(C)] `RelationData` with the utils/rel.h field layout
//! (PostgreSQL 18.3), so consumers can read rd_rel/rd_att/rd_id/rd_locator etc.
//! directly. It replaces the zero-sized stub `RelationData` that previously lived
//! in nodes/execnodes.rs (which now re-exports from here).
//!
//! #include mapping (rel.h):
//!   "storage/relfilelocator.h" -> RelFileLocator (canonical ported shape lives
//!                                 in crate::common::blkreftable; no standalone
//!                                 storage/relfilelocator module exists yet).
//!   "catalog/pg_class.h"       -> crate::catalog::pg_class::Form_pg_class
//!   "catalog/pg_index.h"       -> crate::catalog::pg_index::Form_pg_index
//!   "access/tupdesc.h"         -> crate::access::common::tupdesc::TupleDesc
//!   "nodes/bitmapset.h"        -> crate::nodes::bitmapset::Bitmapset
//!   utils/fmgr.h (FmgrInfo)    -> crate::utils::fmgr::FmgrInfo
//!   nodes/pg_list.h (List)     -> crate::nodes::pg_list::List
//!   access/amapi.h             -> crate::access::index::amapi::IndexAmRoutine
//!
//! LockRelId / LockInfoData "really belong to lmgr.h" but, as in the C header,
//! are declared here so a Relation can embed a LockInfoData field.
//!
//! Fields whose pointee types are not yet ported are kept as pointer-sized
//! opaque pointers (`*mut c_void` / `*const c_void`) so the overall struct
//! layout stays byte-exact: SMgrRelation, RuleLock*, MemoryContext,
//! TriggerDesc*, RowSecurityDesc*, PartitionKey, PartitionDesc, PublicationDesc*,
//! bytea*/bytea**, TableAmRoutine*, HeapTupleData*, FdwRoutine*,
//! PgStat_TableStatus*, void*. Pointers to PORTED types keep their real pointee.

use crate::prelude::*;

use crate::access::common::tupdesc::TupleDesc;
use crate::access::index::amapi::IndexAmRoutine;
use crate::catalog::pg_class::Form_pg_class;
use crate::catalog::pg_index::Form_pg_index;
use crate::storage::relfilelocator::RelFileLocator; // storage/relfilelocator.h (canonical)
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::pg_list::List;
use crate::utils::fmgr::FmgrInfo;

// Scalar type aliases used by rel.h (mirroring their C typedefs).
pub type ProcNumber = c_int; // storage/procnumber.h
pub type SubTransactionId = uint32; // c.h
pub type TransactionId = uint32; // c.h
pub type RegProcedure = Oid; // postgres_ext.h: regproc/RegProcedure is an Oid

// One opaque pointer alias for unported pointee types (layout-exact).
pub type OpaquePtr = *mut c_void;

/*
 * LockRelId and LockInfo really belong to lmgr.h, but it's more convenient
 * to declare them here so we can have a LockInfoData field in a Relation.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LockRelId {
    pub relId: Oid, /* a relation identifier */
    pub dbId: Oid,  /* a database identifier */
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LockInfoData {
    pub lockRelId: LockRelId,
}

pub type LockInfo = *mut LockInfoData;

/*
 * Here are the contents of a relation cache entry.
 */
#[repr(C)]
pub struct RelationData {
    pub rd_locator: RelFileLocator, /* relation physical identifier */
    pub rd_smgr: OpaquePtr,         /* SMgrRelation: cached file handle, or NULL */
    pub rd_refcnt: c_int,           /* reference count */
    pub rd_backend: ProcNumber,     /* owning backend's proc number, if temp rel */
    pub rd_islocaltemp: bool,       /* rel is a temp rel of this session */
    pub rd_isnailed: bool,          /* rel is nailed in cache */
    pub rd_isvalid: bool,           /* relcache entry is valid */
    pub rd_indexvalid: bool,        /* is rd_indexlist valid? (also rd_pkindex and
                                     * rd_replidindex) */
    pub rd_statvalid: bool, /* is rd_statlist valid? */

    /*
     * rd_createSubid is the ID of the highest subtransaction the rel has
     * survived into or zero if the rel or its storage was created before the
     * current top transaction.  See the C header for the full discussion of
     * the rd_*Subid fields; their accuracy is critical to RelationNeedsWAL().
     */
    pub rd_createSubid: SubTransactionId, /* rel was created in current xact */
    pub rd_newRelfilelocatorSubid: SubTransactionId, /* highest subxact changing
                                           * rd_locator to current value */
    pub rd_firstRelfilelocatorSubid: SubTransactionId, /* highest subxact changing
                                             * rd_locator to any value */
    pub rd_droppedSubid: SubTransactionId, /* dropped with another Subid set */

    pub rd_rel: Form_pg_class,    /* RELATION tuple */
    pub rd_att: TupleDesc,        /* tuple descriptor */
    pub rd_id: Oid,               /* relation's object id */
    pub rd_lockInfo: LockInfoData, /* lock mgr's info for locking relation */
    pub rd_rules: OpaquePtr,      /* RuleLock*: rewrite rules */
    pub rd_rulescxt: OpaquePtr,   /* MemoryContext: private cxt for rd_rules */
    pub trigdesc: OpaquePtr,      /* TriggerDesc*: trigger info, or NULL */
    pub rd_rsdesc: OpaquePtr,     /* RowSecurityDesc*: row security policies */

    /* data managed by RelationGetFKeyList: */
    pub rd_fkeylist: *mut List, /* list of ForeignKeyCacheInfo (see below) */
    pub rd_fkeyvalid: bool,     /* true if list has been computed */

    /* data managed by RelationGetPartitionKey: */
    pub rd_partkey: OpaquePtr,    /* PartitionKey: partition key, or NULL */
    pub rd_partkeycxt: OpaquePtr, /* MemoryContext: private cxt for rd_partkey */

    /* data managed by RelationGetPartitionDesc: */
    pub rd_partdesc: OpaquePtr, /* PartitionDesc: partition descriptor, or NULL */
    pub rd_pdcxt: OpaquePtr,    /* MemoryContext: private cxt for rd_partdesc */

    /* Same as above, for partdescs that omit detached partitions */
    pub rd_partdesc_nodetached: OpaquePtr, /* PartitionDesc w/o detached parts */
    pub rd_pddcxt: OpaquePtr,              /* MemoryContext for rd_partdesc_nodetached */

    /*
     * pg_inherits.xmin of the partition that was excluded in
     * rd_partdesc_nodetached.
     */
    pub rd_partdesc_nodetached_xmin: TransactionId,

    /* data managed by RelationGetPartitionQual: */
    pub rd_partcheck: *mut List,    /* partition CHECK quals */
    pub rd_partcheckvalid: bool,    /* true if list has been computed */
    pub rd_partcheckcxt: OpaquePtr, /* MemoryContext: private cxt for rd_partcheck */

    /* data managed by RelationGetIndexList: */
    pub rd_indexlist: *mut List, /* list of OIDs of indexes on relation */
    pub rd_pkindex: Oid,         /* OID of (deferrable?) primary key, if any */
    pub rd_ispkdeferrable: bool, /* is rd_pkindex a deferrable PK? */
    pub rd_replidindex: Oid,     /* OID of replica identity index, if any */

    /* data managed by RelationGetStatExtList: */
    pub rd_statlist: *mut List, /* list of OIDs of extended stats */

    /* data managed by RelationGetIndexAttrBitmap: */
    pub rd_attrsvalid: bool,             /* are bitmaps of attrs valid? */
    pub rd_keyattr: *mut Bitmapset,      /* cols that can be ref'd by foreign keys */
    pub rd_pkattr: *mut Bitmapset,       /* cols included in primary key */
    pub rd_idattr: *mut Bitmapset,       /* included in replica identity index */
    pub rd_hotblockingattr: *mut Bitmapset, /* cols blocking HOT update */
    pub rd_summarizedattr: *mut Bitmapset, /* cols indexed by summarizing indexes */

    pub rd_pubdesc: OpaquePtr, /* PublicationDesc*: publication descriptor, or NULL */

    /*
     * rd_options is set whenever rd_rel is loaded into the relcache entry.
     * NULL means "use defaults".
     */
    pub rd_options: OpaquePtr, /* bytea*: parsed pg_class.reloptions */

    /*
     * Oid of the handler for this relation. For an index this is a function
     * returning IndexAmRoutine, for table like relations a function returning
     * TableAmRoutine.
     */
    pub rd_amhandler: Oid, /* OID of index AM's handler function */

    /*
     * Table access method.
     */
    pub rd_tableam: *const c_void, /* const struct TableAmRoutine * */

    /* These are non-NULL only for an index relation: */
    pub rd_index: Form_pg_index,  /* pg_index tuple describing this index */
    pub rd_indextuple: OpaquePtr, /* HeapTupleData*: all of pg_index tuple */

    /*
     * index access support info (used only for an index relation)
     */
    pub rd_indexcxt: OpaquePtr,         /* MemoryContext: private cxt for this stuff */
    pub rd_indam: *mut IndexAmRoutine,  /* index AM's API struct */
    pub rd_opfamily: *mut Oid,          /* OIDs of op families for each index col */
    pub rd_opcintype: *mut Oid,         /* OIDs of opclass declared input data types */
    pub rd_support: *mut RegProcedure,  /* OIDs of support procedures */
    pub rd_supportinfo: *mut FmgrInfo,  /* lookup info for support procedures */
    pub rd_indoption: *mut i16,         /* per-column AM-specific flags */
    pub rd_indexprs: *mut List,         /* index expression trees, if any */
    pub rd_indpred: *mut List,          /* index predicate tree, if any */
    pub rd_exclops: *mut Oid,           /* OIDs of exclusion operators, if any */
    pub rd_exclprocs: *mut Oid,         /* OIDs of exclusion ops' procs, if any */
    pub rd_exclstrats: *mut uint16,     /* exclusion ops' strategy numbers, if any */
    pub rd_indcollation: *mut Oid,      /* OIDs of index collations */
    pub rd_opcoptions: *mut *mut c_void, /* bytea**: parsed opclass-specific options */

    /*
     * rd_amcache is available for index and table AMs to cache private data
     * about the relation.
     */
    pub rd_amcache: *mut c_void, /* available for use by index/table AM */

    /*
     * foreign-table support
     */
    pub rd_fdwroutine: OpaquePtr, /* FdwRoutine*: cached function pointers, or NULL */

    /*
     * Hack for CLUSTER, rewriting ALTER TABLE, etc.
     */
    pub rd_toastoid: Oid, /* Real TOAST table's OID, or InvalidOid */

    pub pgstat_enabled: bool,    /* should relation stats be counted */
    pub pgstat_info: OpaquePtr,  /* PgStat_TableStatus*: statistics collection area */
}

/* utils/relcache.h: Relation is a pointer to RelationData. */
pub type Relation = *mut RelationData;

/* utils/relcache.h: RelationPtr is a pointer to a Relation (array element). */
pub type RelationPtr = *mut Relation;

/* InvalidRelation: a NULL Relation. */
pub const InvalidRelation: Relation = null_mut();

/*
 * RelationIsValid
 *		True iff relation descriptor is valid.
 */
#[inline]
pub unsafe fn RelationIsValid(relation: Relation) -> bool {
    !relation.is_null()
}

/*
 * RelationHasReferenceCountZero
 *		True iff relation reference count is zero.
 */
#[inline]
pub unsafe fn RelationHasReferenceCountZero(relation: Relation) -> bool {
    (*relation).rd_refcnt == 0
}

/*
 * RelationGetForm
 *		Returns pg_class tuple for a relation.
 */
#[inline]
pub unsafe fn RelationGetForm(relation: Relation) -> Form_pg_class {
    (*relation).rd_rel
}

/*
 * RelationGetRelid
 *		Returns the OID of the relation.
 */
#[inline]
pub unsafe fn RelationGetRelid(relation: Relation) -> Oid {
    (*relation).rd_id
}

/*
 * RelationGetNumberOfAttributes
 *		Returns the total number of attributes in a relation.
 *
 * Note: relnatts is an int16 in pg_class; C yields it as plain int, so we
 * widen to c_int to match the macro's value type.
 */
#[inline]
pub unsafe fn RelationGetNumberOfAttributes(relation: Relation) -> c_int {
    (*(*relation).rd_rel).relnatts as c_int
}

/*
 * RelationGetDescr
 *		Returns tuple descriptor for a relation.
 */
#[inline]
#[no_mangle]
pub unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc {
    (*relation).rd_att
}

/*
 * RelationGetRelationName
 *		Returns the rel's name.
 *
 * The C macro expands to NameStr((relation)->rd_rel->relname), i.e. a pointer
 * to the embedded NameData char array in the pg_class form.  We return a
 * *mut c_char pointing at that array's first byte.
 */
#[inline]
#[no_mangle]
pub unsafe fn RelationGetRelationName(relation: Relation) -> *mut c_char {
    &mut (*(*relation).rd_rel).relname as *mut _ as *mut c_char
}

/*
 * RelationGetNamespace
 *		Returns the rel's namespace OID.
 */
#[inline]
pub unsafe fn RelationGetNamespace(relation: Relation) -> Oid {
    (*(*relation).rd_rel).relnamespace
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::MaybeUninit;

    #[test]
    fn lockinfodata_is_eight_bytes() {
        assert_eq!(core::mem::size_of::<LockInfoData>(), 8);
    }

    #[test]
    fn relation_get_relid_reads_back() {
        // Build a zeroed RelationData, set rd_id, read it back via the accessor.
        let mut rel: MaybeUninit<RelationData> = MaybeUninit::zeroed();
        unsafe {
            (*rel.as_mut_ptr()).rd_id = 0x1234_5678;
            let p: Relation = rel.as_mut_ptr();
            assert_eq!(RelationGetRelid(p), 0x1234_5678);
            assert!(RelationIsValid(p));
        }
    }
}
