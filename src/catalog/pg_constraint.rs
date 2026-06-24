//! Translated from PostgreSQL src/include/catalog/pg_constraint.h

use crate::access::attnum::AttrNumber;
use crate::access::htup::HeapTuple;
use crate::c::{text, NameData};
use crate::nodes::bitmapset::Bitmapset;
use crate::postgres_ext::Oid;

pub const ConstraintRelationId: Oid = Oid(2606);

// pg_node_tree catalog field = varlena (serialized node tree); modeled as text.
pub type PgNodeTree = text; // TODO(struct-forward)

#[repr(C)]
pub struct FormData_pg_constraint {
    pub oid: Oid,
    pub conname: NameData,
    pub connamespace: Oid, // BKI_LOOKUP(pg_namespace)
    pub contype: i8,       // constraint type; see CONSTRAINT_*
    pub condeferrable: bool,
    pub condeferred: bool,
    pub conenforced: bool,
    pub convalidated: bool,
    pub conrelid: Oid,    // BKI_LOOKUP_OPT(pg_class)
    pub contypid: Oid,    // BKI_LOOKUP_OPT(pg_type)
    pub conindid: Oid,    // BKI_LOOKUP_OPT(pg_class)
    pub conparentid: Oid, // BKI_LOOKUP_OPT(pg_constraint)
    pub confrelid: Oid,   // BKI_LOOKUP_OPT(pg_class)
    pub confupdtype: i8,
    pub confdeltype: i8,
    pub confmatchtype: i8,
    pub conislocal: bool,
    pub coninhcount: i16,
    pub connoinherit: bool,
    pub conperiod: bool,
    // CATALOG_VARLEN (not in fixed part):
    pub conkey: [i16; 1],         // columns of conrelid constrained
    pub confkey: [i16; 1],        // FK: referenced columns of confrelid
    pub conpfeqop: [Oid; 1],      // FK: PK = FK eq/overlap ops; BKI_LOOKUP(pg_operator)
    pub conppeqop: [Oid; 1],      // FK: PK = PK eq/overlap ops; BKI_LOOKUP(pg_operator)
    pub conffeqop: [Oid; 1],      // FK: FK = FK eq/overlap ops; BKI_LOOKUP(pg_operator)
    pub confdelsetcols: [i16; 1], // FK ON DELETE SET subset of conkey
    pub conexclop: [Oid; 1],      // exclusion ops; BKI_LOOKUP(pg_operator)
    pub conbin: PgNodeTree,       // check constraint expr (nodeToString)
}

pub type Form_pg_constraint = *mut FormData_pg_constraint; // TODO(ptr)

// DECLARE_TOAST(pg_constraint, 2832, 2833)
// DECLARE_INDEX(pg_constraint_conname_nsp_index, 2664, ConstraintNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_constraint_conrelid_contypid_conname_index, 2665, ConstraintRelidTypidNameIndexId, ...)
// DECLARE_INDEX(pg_constraint_contypid_index, 2666, ConstraintTypidIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_constraint_oid_index, 2667, ConstraintOidIndexId, ...)
// DECLARE_INDEX(pg_constraint_conparentid_index, 2579, ConstraintParentIndexId, ...)
// MAKE_SYSCACHE(CONSTROID, pg_constraint_oid_index, 16)
// DECLARE_ARRAY_FOREIGN_KEY_OPT((conrelid, conkey), pg_attribute, (attrelid, attnum))
// DECLARE_ARRAY_FOREIGN_KEY((confrelid, confkey), pg_attribute, (attrelid, attnum))

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_constraint_oid: i32 = 1;
pub const Anum_pg_constraint_conname: i32 = 2;
pub const Anum_pg_constraint_connamespace: i32 = 3;
pub const Anum_pg_constraint_contype: i32 = 4;
pub const Anum_pg_constraint_condeferrable: i32 = 5;
pub const Anum_pg_constraint_condeferred: i32 = 6;
pub const Anum_pg_constraint_conenforced: i32 = 7;
pub const Anum_pg_constraint_convalidated: i32 = 8;
pub const Anum_pg_constraint_conrelid: i32 = 9;
pub const Anum_pg_constraint_contypid: i32 = 10;
pub const Anum_pg_constraint_conindid: i32 = 11;
pub const Anum_pg_constraint_conparentid: i32 = 12;
pub const Anum_pg_constraint_confrelid: i32 = 13;
pub const Anum_pg_constraint_confupdtype: i32 = 14;
pub const Anum_pg_constraint_confdeltype: i32 = 15;
pub const Anum_pg_constraint_confmatchtype: i32 = 16;
pub const Anum_pg_constraint_conislocal: i32 = 17;
pub const Anum_pg_constraint_coninhcount: i32 = 18;
pub const Anum_pg_constraint_connoinherit: i32 = 19;
pub const Anum_pg_constraint_conperiod: i32 = 20;
pub const Anum_pg_constraint_conkey: i32 = 21;
pub const Anum_pg_constraint_confkey: i32 = 22;
pub const Anum_pg_constraint_conpfeqop: i32 = 23;
pub const Anum_pg_constraint_conppeqop: i32 = 24;
pub const Anum_pg_constraint_conffeqop: i32 = 25;
pub const Anum_pg_constraint_confdelsetcols: i32 = 26;
pub const Anum_pg_constraint_conexclop: i32 = 27;
pub const Anum_pg_constraint_conbin: i32 = 28;
pub const Natts_pg_constraint: i32 = 28;

// Valid values for contype.
pub const CONSTRAINT_CHECK: i8 = b'c' as i8;
pub const CONSTRAINT_FOREIGN: i8 = b'f' as i8;
pub const CONSTRAINT_NOTNULL: i8 = b'n' as i8;
pub const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
pub const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
pub const CONSTRAINT_TRIGGER: i8 = b't' as i8;
pub const CONSTRAINT_EXCLUSION: i8 = b'x' as i8;

/// Identify constraint type for lookup purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintCategory {
    CONSTRAINT_RELATION,
    CONSTRAINT_DOMAIN,
    CONSTRAINT_ASSERTION, // for future expansion
}

// Forward refs for the function stubs; repointed in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::primnodes::Node in Phase 2")]
pub struct Node; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::pg_list::List in Phase 2")]
pub struct List; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::catalog::objectaddress::ObjectAddresses in Phase 2")]
pub struct ObjectAddresses; // TODO(struct-forward)

/// Outputs of DeconstructFkConstraintRow (C used 8 out-params).
pub struct FkConstraintRow {
    pub numfks: i32,
    pub conkey: Vec<AttrNumber>,
    pub confkey: Vec<AttrNumber>,
    pub pf_eq_oprs: Vec<Oid>,
    pub pp_eq_oprs: Vec<Oid>,
    pub ff_eq_oprs: Vec<Oid>,
    pub num_fk_del_set_cols: i32,
    pub fk_del_set_cols: Vec<AttrNumber>,
}

/// Outputs of FindFKPeriodOpers (C used 3 out-params).
pub struct FkPeriodOpers {
    pub containedbyoperoid: Oid,
    pub aggedcontainedbyoperoid: Oid,
    pub intersectoperoid: Oid,
}

#[allow(deprecated)]
pub fn CreateConstraintEntry(
    _constraint_name: &str,
    _constraint_namespace: Oid,
    _constraint_type: i8,
    _is_deferrable: bool,
    _is_deferred: bool,
    _is_enforced: bool,
    _is_validated: bool,
    _parent_constr_id: Oid,
    _rel_id: Oid,
    _constraint_key: &[i16],
    _constraint_n_total_keys: i32,
    _domain_id: Oid,
    _index_rel_id: Oid,
    _foreign_rel_id: Oid,
    _foreign_key: &[i16],
    _pf_eq_op: &[Oid],
    _pp_eq_op: &[Oid],
    _ff_eq_op: &[Oid],
    _foreign_update_type: i8,
    _foreign_delete_type: i8,
    _fk_delete_set_cols: &[i16],
    _foreign_match_type: i8,
    _excl_op: &[Oid],
    _con_expr: &Node,
    _con_bin: &str,
    _con_is_local: bool,
    _con_inh_count: i16,
    _con_no_inherit: bool,
    _con_period: bool,
    _is_internal: bool,
) -> Oid {
    unimplemented!()
}

pub fn ConstraintNameIsUsed(_con_cat: ConstraintCategory, _obj_id: Oid, _conname: &str) -> bool {
    unimplemented!()
}

pub fn ConstraintNameExists(_conname: &str, _namespaceid: Oid) -> bool {
    unimplemented!()
}

pub fn ChooseConstraintName(
    _name1: &str,
    _name2: &str,
    _label: &str,
    _namespaceid: Oid,
    _others: &List,
) -> String {
    unimplemented!()
}

pub fn findNotNullConstraintAttnum(_relid: Oid, _attnum: AttrNumber) -> HeapTuple {
    unimplemented!()
}

pub fn findNotNullConstraint(_relid: Oid, _colname: &str) -> HeapTuple {
    unimplemented!()
}

pub fn findDomainNotNullConstraint(_typid: Oid) -> HeapTuple {
    unimplemented!()
}

pub fn extractNotNullColumn(_constr_tup: HeapTuple) -> AttrNumber {
    unimplemented!()
}

pub fn AdjustNotNullInheritance(
    _relid: Oid,
    _attnum: AttrNumber,
    _new_conname: &str,
    _is_local: bool,
    _is_no_inherit: bool,
    _is_notvalid: bool,
) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn RelationGetNotNullConstraints(_relid: Oid, _cooked: bool, _include_noinh: bool) -> List {
    unimplemented!()
}

pub fn RemoveConstraintById(_con_id: Oid) {
    unimplemented!()
}

pub fn RenameConstraintById(_con_id: Oid, _newname: &str) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn AlterConstraintNamespaces(
    _owner_id: Oid,
    _old_nsp_id: Oid,
    _new_nsp_id: Oid,
    _is_type: bool,
    _objs_moved: &mut ObjectAddresses,
) {
    unimplemented!()
}

pub fn ConstraintSetParentConstraint(_child_constr_id: Oid, _parent_constr_id: Oid, _child_table_id: Oid) {
    unimplemented!()
}

/// `missing_ok` -> None when not found (C returned InvalidOid).
pub fn get_relation_constraint_oid(_relid: Oid, _conname: &str) -> Option<Oid> {
    unimplemented!()
}

/// C also returned the constraint OID via `Oid *constraintOid`; folded into tuple.
pub fn get_relation_constraint_attnos(_relid: Oid, _conname: &str) -> Option<(Bitmapset, Oid)> {
    unimplemented!()
}

pub fn get_domain_constraint_oid(_typid: Oid, _conname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn get_relation_idx_constraint_oid(_relation_id: Oid, _index_id: Oid) -> Oid {
    unimplemented!()
}

/// C returned the constraint OID via `Oid *constraintOid`; folded into tuple.
pub fn get_primary_key_attnos(_relid: Oid, _deferrable_ok: bool) -> Option<(Bitmapset, Oid)> {
    unimplemented!()
}

pub fn DeconstructFkConstraintRow(_tuple: HeapTuple) -> FkConstraintRow {
    unimplemented!()
}

pub fn FindFKPeriodOpers(_opclass: Oid) -> FkPeriodOpers {
    unimplemented!()
}

/// C threaded constraint deps back through `List **constraintDeps`; returned here.
#[allow(deprecated)]
pub fn check_functional_grouping(
    _relid: Oid,
    _varno: usize,
    _varlevelsup: usize,
    _grouping_columns: &List,
) -> (bool, List) {
    unimplemented!()
}
