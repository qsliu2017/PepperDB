//! Translated from PostgreSQL src/include/catalog/pg_inherits.h

use crate::c::TransactionId;
use crate::postgres_ext::Oid;
use crate::storage::lock::LOCKMODE;

pub const InheritsRelationId: Oid = Oid(2611);

/// pg_inherits has no implicit oid column.
#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_inherits {
    pub inhrelid: Oid,   // BKI_LOOKUP(pg_class)
    pub inhparent: Oid,  // BKI_LOOKUP(pg_class)
    pub inhseqno: i32,
    pub inhdetachpending: bool,
}

pub type Form_pg_inherits = *mut FormData_pg_inherits; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_inherits_relid_seqno_index, 2680, InheritsRelidSeqnoIndexId, pg_inherits, btree(inhrelid oid_ops, inhseqno int4_ops))
// DECLARE_INDEX(pg_inherits_parent_index, 2187, InheritsParentIndexId, pg_inherits, btree(inhparent oid_ops))

/// Outputs of `find_inheritance_children_extended` beyond the child list.
pub struct DetachedInfo {
    pub detached_exist: bool,
    pub detached_xmin: TransactionId,
}

pub fn find_inheritance_children(_parent_rel_id: Oid, _lockmode: LOCKMODE) -> Vec<Oid> {
    unimplemented!()
}

/// Returns the child OIDs plus the optional detached-partition info.
pub fn find_inheritance_children_extended(
    _parent_rel_id: Oid,
    _omit_detached: bool,
    _lockmode: LOCKMODE,
) -> (Vec<Oid>, DetachedInfo) {
    unimplemented!()
}

/// Returns all inheritors plus, per result entry, the number of parents.
pub fn find_all_inheritors(_parent_rel_id: Oid, _lockmode: LOCKMODE) -> (Vec<Oid>, Vec<i32>) {
    unimplemented!()
}

pub fn has_subclass(_relation_id: Oid) -> bool {
    unimplemented!()
}

pub fn has_superclass(_relation_id: Oid) -> bool {
    unimplemented!()
}

pub fn typeInheritsFrom(_subclass_type_id: Oid, _superclass_type_id: Oid) -> bool {
    unimplemented!()
}

pub fn StoreSingleInheritance(_relation_id: Oid, _parent_oid: Oid, _seq_number: i32) {
    unimplemented!()
}

pub fn DeleteInheritsTuple(
    _inhrelid: Oid,
    _inhparent: Oid,
    _expect_detach_pending: bool,
    _childname: Option<&str>,
) -> bool {
    unimplemented!()
}

pub fn PartitionHasPendingDetach(_partoid: Oid) -> bool {
    unimplemented!()
}
