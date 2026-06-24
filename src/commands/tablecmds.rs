//! Translated from PostgreSQL src/include/commands/tablecmds.h

use crate::access::htup::HeapTuple;
use crate::access::tupdesc::TupleDesc;
use crate::catalog::dependency::ObjectAddresses;
use crate::catalog::objectaddress::ObjectAddress;
use crate::c::SubTransactionId;
use crate::common::relpath::RelFileNumber;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    AlterObjectSchemaStmt, AlterTableMoveAllStmt, AlterTableStmt, CreateStmt, DropBehavior,
    DropStmt, RenameStmt, TruncateStmt,
};
use crate::nodes::primnodes::{OnCommitAction, RangeVar};
use crate::postgres_ext::Oid;
use crate::storage::lock::LOCKMODE;
use crate::utils::relcache::Relation;

// TODO(struct-forward): real definition is in tcop/utility.h (not yet translated).
#[deprecated(note = "TODO(struct-forward): repoint to crate::tcop::utility::AlterTableUtilityContext in Phase 2")]
pub struct AlterTableUtilityContext;

// out-param typaddress folded into the returned tuple.
pub fn DefineRelation(
    _stmt: &CreateStmt,
    _relkind: i8,
    _ownerId: Oid,
    _queryString: &str,
) -> (ObjectAddress, ObjectAddress) {
    unimplemented!()
}

pub fn BuildDescForRelation(_columns: &[Box<Node>]) -> TupleDesc {
    unimplemented!()
}

pub fn RemoveRelations(_drop: &DropStmt) {
    unimplemented!()
}

pub fn AlterTableLookupRelation(_stmt: &AlterTableStmt, _lockmode: LOCKMODE) -> Oid {
    unimplemented!()
}

#[allow(deprecated)]
pub fn AlterTable(
    _stmt: &AlterTableStmt,
    _lockmode: LOCKMODE,
    _context: &mut AlterTableUtilityContext,
) {
    unimplemented!()
}

pub fn AlterTableGetLockLevel(_cmds: &[Box<Node>]) -> LOCKMODE {
    unimplemented!()
}

pub fn ATExecChangeOwner(
    _relationOid: Oid,
    _newOwnerId: Oid,
    _recursing: bool,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

pub fn AlterTableInternal(_relid: Oid, _cmds: &[Box<Node>], _recurse: bool) {
    unimplemented!()
}

pub fn AlterTableMoveAll(_stmt: &AlterTableMoveAllStmt) -> Oid {
    unimplemented!()
}

// out-param oldschema folded into the returned tuple.
pub fn AlterTableNamespace(_stmt: &AlterObjectSchemaStmt) -> (ObjectAddress, Oid) {
    unimplemented!()
}

pub fn AlterTableNamespaceInternal(
    _rel: Relation,
    _oldNspOid: Oid,
    _nspOid: Oid,
    _objsMoved: &mut ObjectAddresses,
) {
    unimplemented!()
}

pub fn AlterRelationNamespaceInternal(
    _classRel: Relation,
    _relOid: Oid,
    _oldNspOid: Oid,
    _newNspOid: Oid,
    _hasDependEntry: bool,
    _objsMoved: &mut ObjectAddresses,
) {
    unimplemented!()
}

pub fn CheckTableNotInUse(_rel: Relation, _stmt: &str) {
    unimplemented!()
}

pub fn ExecuteTruncate(_stmt: &TruncateStmt) {
    unimplemented!()
}

pub fn ExecuteTruncateGuts(
    _explicit_rels: &[Box<Node>],
    _relids: &[Box<Node>],
    _relids_logged: &[Box<Node>],
    _behavior: DropBehavior,
    _restart_seqs: bool,
    _run_as_table_owner: bool,
) {
    unimplemented!()
}

pub fn SetRelationHasSubclass(_relationId: Oid, _relhassubclass: bool) {
    unimplemented!()
}

pub fn CheckRelationTableSpaceMove(_rel: Relation, _newTableSpaceId: Oid) -> bool {
    unimplemented!()
}

pub fn SetRelationTableSpace(
    _rel: Relation,
    _newTableSpaceId: Oid,
    _newRelFilenumber: RelFileNumber,
) {
    unimplemented!()
}

pub fn renameatt(_stmt: &RenameStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn RenameConstraint(_stmt: &RenameStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn RenameRelation(_stmt: &RenameStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn RenameRelationInternal(
    _myrelid: Oid,
    _newrelname: &str,
    _is_internal: bool,
    _is_index: bool,
) {
    unimplemented!()
}

pub fn ResetRelRewrite(_myrelid: Oid) {
    unimplemented!()
}

pub fn find_composite_type_dependencies(
    _typeOid: Oid,
    _origRelation: Relation,
    _origTypeName: &str,
) {
    unimplemented!()
}

pub fn check_of_type(_typetuple: HeapTuple) {
    unimplemented!()
}

pub fn register_on_commit_action(_relid: Oid, _action: OnCommitAction) {
    unimplemented!()
}

pub fn remove_on_commit_action(_relid: Oid) {
    unimplemented!()
}

pub fn PreCommit_on_commit_actions() {
    unimplemented!()
}

pub fn AtEOXact_on_commit_actions(_isCommit: bool) {
    unimplemented!()
}

pub fn AtEOSubXact_on_commit_actions(
    _isCommit: bool,
    _mySubid: SubTransactionId,
    _parentSubid: SubTransactionId,
) {
    unimplemented!()
}

// void *arg opaque callback context -> closure-captured state in Phase 2.
pub fn RangeVarCallbackMaintainsTable(
    _relation: &RangeVar,
    _relId: Oid,
    _oldRelId: Oid,
    _arg: &mut dyn std::any::Any,
) {
    unimplemented!()
}

pub fn RangeVarCallbackOwnsRelation(
    _relation: &RangeVar,
    _relId: Oid,
    _oldRelId: Oid,
    _arg: &mut dyn std::any::Any,
) {
    unimplemented!()
}

pub fn PartConstraintImpliedByRelConstraint(
    _scanrel: Relation,
    _partConstraint: &[Box<Node>],
) -> bool {
    unimplemented!()
}
