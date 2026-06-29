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
use crate::tcop::utility::AlterTableUtilityContext;
use crate::utils::rel::RelationData;

// out-param typaddress folded into the returned tuple.
pub fn DefineRelation(
    _stmt: &CreateStmt,
    _relkind: i8,
    _ownerId: Oid,
    _queryString: &str,
) -> (ObjectAddress, ObjectAddress) {
    unimplemented!()
}

pub fn BuildDescForRelation(_columns: &[Node]) -> TupleDesc {
    unimplemented!()
}

pub fn RemoveRelations(_drop: &DropStmt) {
    unimplemented!()
}

pub fn AlterTableLookupRelation(_stmt: &AlterTableStmt, _lockmode: LOCKMODE) -> Oid {
    unimplemented!()
}

pub fn AlterTable(
    _stmt: &AlterTableStmt,
    _lockmode: LOCKMODE,
    _context: &mut AlterTableUtilityContext,
) {
    unimplemented!()
}

pub fn AlterTableGetLockLevel(_cmds: &[Node]) -> LOCKMODE {
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

pub fn AlterTableInternal(_relid: Oid, _cmds: &[Node], _recurse: bool) {
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
    _rel: &RelationData,
    _oldNspOid: Oid,
    _nspOid: Oid,
    _objsMoved: &mut ObjectAddresses,
) {
    unimplemented!()
}

pub fn AlterRelationNamespaceInternal(
    _classRel: &RelationData,
    _relOid: Oid,
    _oldNspOid: Oid,
    _newNspOid: Oid,
    _hasDependEntry: bool,
    _objsMoved: &mut ObjectAddresses,
) {
    unimplemented!()
}

pub fn CheckTableNotInUse(_rel: &RelationData, _stmt: &str) {
    unimplemented!()
}

pub fn ExecuteTruncate(_stmt: &TruncateStmt) {
    unimplemented!()
}

pub fn ExecuteTruncateGuts(
    _explicit_rels: &[Node],
    _relids: &[Node],
    _relids_logged: &[Node],
    _behavior: DropBehavior,
    _restart_seqs: bool,
    _run_as_table_owner: bool,
) {
    unimplemented!()
}

pub fn SetRelationHasSubclass(_relationId: Oid, _relhassubclass: bool) {
    unimplemented!()
}

pub fn CheckRelationTableSpaceMove(_rel: &RelationData, _newTableSpaceId: Oid) -> bool {
    unimplemented!()
}

pub fn SetRelationTableSpace(
    _rel: &RelationData,
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
    _origRelation: &RelationData,
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
    _scanrel: &RelationData,
    _partConstraint: &[Node],
) -> bool {
    unimplemented!()
}
