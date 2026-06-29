//! Translated from PostgreSQL src/include/catalog/pg_operator.h

#![allow(
    clippy::fn_params_excessive_bools,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::htup::HeapTuple;
use crate::c::{regproc, NameData};
use crate::catalog::objectaddress::ObjectAddress;
use crate::postgres_ext::Oid;

pub const OperatorRelationId: Oid = Oid(2617);
/// pg_operator's composite (row) type OID. genbki auto-assigns catalog rowtypes;
/// the value is only stored as the nailed descriptor's `tdtypeid` (not load-bearing
/// for M3 resolution).
pub const OperatorRelation_Rowtype_Id: Oid = Oid(11631);

// DECLARE_UNIQUE_INDEX_PKEY(pg_operator_oid_index, 2688, OperatorOidIndexId)
pub const OperatorOidIndexId: Oid = Oid(2688);
// DECLARE_UNIQUE_INDEX(pg_operator_oprname_l_r_n_index, 2689, OperatorNameNspIndexId)
pub const OperatorNameNspIndexId: Oid = Oid(2689);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_operator {
    pub oid: Oid,
    pub oprname: NameData,
    pub oprnamespace: Oid, // BKI_LOOKUP(pg_namespace)
    pub oprowner: Oid,     // BKI_LOOKUP(pg_authid)
    pub oprkind: i8,       // 'l' prefix or 'b' infix
    pub oprcanmerge: bool,
    pub oprcanhash: bool,
    pub oprleft: Oid,   // BKI_LOOKUP_OPT(pg_type)
    pub oprright: Oid,  // BKI_LOOKUP(pg_type)
    pub oprresult: Oid, // BKI_LOOKUP_OPT(pg_type)
    pub oprcom: Oid,    // BKI_LOOKUP_OPT(pg_operator)
    pub oprnegate: Oid, // BKI_LOOKUP_OPT(pg_operator)
    pub oprcode: regproc, // BKI_LOOKUP_OPT(pg_proc)
    pub oprrest: regproc, // BKI_LOOKUP_OPT(pg_proc)
    pub oprjoin: regproc, // BKI_LOOKUP_OPT(pg_proc)
}

pub type Form_pg_operator = *mut FormData_pg_operator; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_operator_oid_index, 2688, OperatorOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_operator_oprname_l_r_n_index, 2689, OperatorNameNspIndexId, ...)
// MAKE_SYSCACHE(OPEROID, pg_operator_oid_index, 32)
// MAKE_SYSCACHE(OPERNAMENSP, pg_operator_oprname_l_r_n_index, 256)

// pg_list tombstoned: name-list params become Vec<Node>.
pub type List = Vec<crate::nodes::nodes::Node>;

// returns oper Oid + `defined` out-param -> (oid, defined)
pub fn OperatorLookup(_operator_name: &List, _left_object_id: Oid, _right_object_id: Oid) -> (Oid, bool) {
    unimplemented!()
}

pub fn OperatorCreate(
    _operator_name: &str,
    _operator_namespace: Oid,
    _left_type_id: Oid,
    _right_type_id: Oid,
    _procedure_id: Oid,
    _commutator_name: &List,
    _negator_name: &List,
    _restriction_id: Oid,
    _join_id: Oid,
    _can_merge: bool,
    _can_hash: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn makeOperatorDependencies(
    _tuple: HeapTuple,
    _make_extension_dep: bool,
    _is_update: bool,
) -> ObjectAddress {
    unimplemented!()
}

pub fn OperatorValidateParams(
    _left_type_id: Oid,
    _right_type_id: Oid,
    _oper_result_type: Oid,
    _has_commutator: bool,
    _has_negator: bool,
    _has_restriction_selectivity: bool,
    _has_join_selectivity: bool,
    _can_merge: bool,
    _can_hash: bool,
) {
    unimplemented!()
}

pub fn OperatorUpd(_base_id: Oid, _comm_id: Oid, _neg_id: Oid, _is_delete: bool) {
    unimplemented!()
}
