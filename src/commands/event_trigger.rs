//! Translated from PostgreSQL src/include/commands/event_trigger.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    AlterDefaultPrivilegesStmt, AlterEventTrigStmt, AlterOpFamilyStmt, AlterTSConfigurationStmt,
    CreateEventTrigStmt, CreateOpClassStmt, ObjectType,
};
use crate::postgres_ext::Oid;
use crate::tcop::cmdtaglist::CommandTag;
use crate::utils::aclchk_internal::InternalGrant;
use bitflags::bitflags;

/// EventTriggerData: passed as fmgr "context" when an event trigger fires.
/// In-memory node; the leading C `NodeTag type` field is dropped (the Rust
/// `Node` enum carries the tag).
pub struct EventTriggerData<'a> {
    pub event: &'a str,       // event name
    pub parsetree: Box<Node>, // parse tree
    pub tag: CommandTag,
}

// PGDLLIMPORT bool event_triggers -- process-global GUC.
// TODO(struct-forward): move to Session/task-local state in Phase 2.
pub static event_triggers: bool = false;

bitflags! {
    /// Reasons for relation rewrites (AT_REWRITE_*). Used by
    /// pg_event_trigger_table_rewrite_reason().
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AtRewrite: i32 {
        const ALTER_PERSISTENCE = 0x01;
        const DEFAULT_VAL       = 0x02;
        const COLUMN_REWRITE    = 0x04;
        const ACCESS_METHOD     = 0x08;
    }
}

pub fn CreateEventTrigger(_stmt: &CreateEventTrigStmt) -> Oid {
    unimplemented!()
}

// missing_ok -> Option (InvalidOid sentinel collapses to None).
pub fn get_event_trigger_oid(_trigname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn AlterEventTrigger(_stmt: &AlterEventTrigStmt) -> Oid {
    unimplemented!()
}

pub fn AlterEventTriggerOwner(_name: &str, _newOwnerId: Oid) -> ObjectAddress {
    unimplemented!()
}

pub fn AlterEventTriggerOwner_oid(_arg0: Oid, _newOwnerId: Oid) {
    unimplemented!()
}

pub fn EventTriggerSupportsObjectType(_obtype: ObjectType) -> bool {
    unimplemented!()
}

pub fn EventTriggerSupportsObject(_object: &ObjectAddress) -> bool {
    unimplemented!()
}

pub fn EventTriggerDDLCommandStart(_parsetree: &Node) {
    unimplemented!()
}

pub fn EventTriggerDDLCommandEnd(_parsetree: &Node) {
    unimplemented!()
}

pub fn EventTriggerSQLDrop(_parsetree: &Node) {
    unimplemented!()
}

pub fn EventTriggerTableRewrite(_parsetree: &Node, _tableOid: Oid, _reason: i32) {
    unimplemented!()
}

pub fn EventTriggerOnLogin() {
    unimplemented!()
}

pub fn EventTriggerBeginCompleteQuery() -> bool {
    unimplemented!()
}

pub fn EventTriggerEndCompleteQuery() {
    unimplemented!()
}

pub fn trackDroppedObjectsNeeded() -> bool {
    unimplemented!()
}

pub fn EventTriggerSQLDropAddObject(_object: &ObjectAddress, _original: bool, _normal: bool) {
    unimplemented!()
}

pub fn EventTriggerInhibitCommandCollection() {
    unimplemented!()
}

pub fn EventTriggerUndoInhibitCommandCollection() {
    unimplemented!()
}

pub fn EventTriggerCollectSimpleCommand(
    _address: ObjectAddress,
    _secondaryObject: ObjectAddress,
    _parsetree: &Node,
) {
    unimplemented!()
}

pub fn EventTriggerAlterTableStart(_parsetree: &Node) {
    unimplemented!()
}

pub fn EventTriggerAlterTableRelid(_objectId: Oid) {
    unimplemented!()
}

pub fn EventTriggerCollectAlterTableSubcmd(_subcmd: &Node, _address: ObjectAddress) {
    unimplemented!()
}

pub fn EventTriggerAlterTableEnd() {
    unimplemented!()
}

pub fn EventTriggerCollectGrant(_istmt: &InternalGrant) {
    unimplemented!()
}

pub fn EventTriggerCollectAlterOpFam(
    _stmt: &AlterOpFamilyStmt,
    _opfamoid: Oid,
    _operators: Vec<Box<Node>>,
    _procedures: Vec<Box<Node>>,
) {
    unimplemented!()
}

pub fn EventTriggerCollectCreateOpClass(
    _stmt: &CreateOpClassStmt,
    _opcoid: Oid,
    _operators: Vec<Box<Node>>,
    _procedures: Vec<Box<Node>>,
) {
    unimplemented!()
}

pub fn EventTriggerCollectAlterTSConfig(
    _stmt: &AlterTSConfigurationStmt,
    _cfgId: Oid,
    _dictIds: &[Oid],
) {
    unimplemented!()
}

pub fn EventTriggerCollectAlterDefPrivs(_stmt: &AlterDefaultPrivilegesStmt) {
    unimplemented!()
}
