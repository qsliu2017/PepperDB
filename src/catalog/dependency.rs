//! Translated from PostgreSQL src/include/catalog/dependency.h

use bitflags::bitflags;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::DropBehavior;
use crate::postgres_ext::Oid;
use crate::utils::rel::Relation;

// pg_list is tombstoned; List maps to Vec. Element type unknown at this stub
// level, so default to Vec<()>.
pub type List<T = ()> = Vec<T>;
pub type AttrNumber = i16; // c.h AttrNumber

// Stored in a "char" field in pg_depend, so values are ASCII codes.
#[repr(i8)]
pub enum DependencyType {
    Normal = b'n' as i8,
    Auto = b'a' as i8,
    Internal = b'i' as i8,
    PartitionPri = b'P' as i8,
    PartitionSec = b'S' as i8,
    Extension = b'e' as i8,
    AutoExtension = b'x' as i8,
}

// Determines the semantics of a pg_shdepend entry.
#[repr(i8)]
pub enum SharedDependencyType {
    Owner = b'o' as i8,
    Acl = b'a' as i8,
    Initacl = b'i' as i8,
    Policy = b'r' as i8,
    Tablespace = b't' as i8,
    Invalid = 0,
}

// expansible list of ObjectAddresses (private in dependency.c)
pub struct ObjectAddresses; // opaque

bitflags! {
    // flag bits for performDeletion/performMultipleDeletions
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PerformDeletion: i32 {
        const INTERNAL = 0x0001;
        const CONCURRENTLY = 0x0002;
        const QUIETLY = 0x0004;
        const SKIP_ORIGINAL = 0x0008;
        const SKIP_EXTENSIONS = 0x0010;
        const CONCURRENT_LOCK = 0x0020;
    }
}

// in dependency.c
pub fn AcquireDeletionLock(_object: &ObjectAddress, _flags: i32) {
    unimplemented!()
}

pub fn ReleaseDeletionLock(_object: &ObjectAddress) {
    unimplemented!()
}

pub fn performDeletion(_object: &ObjectAddress, _behavior: DropBehavior, _flags: i32) {
    unimplemented!()
}

pub fn performMultipleDeletions(_objects: &ObjectAddresses, _behavior: DropBehavior, _flags: i32) {
    unimplemented!()
}

pub fn recordDependencyOnExpr(
    _depender: &ObjectAddress,
    _expr: &Node,
    _rtable: &List,
    _behavior: DependencyType,
) {
    unimplemented!()
}

pub fn recordDependencyOnSingleRelExpr(
    _depender: &ObjectAddress,
    _expr: &Node,
    _rel_id: Oid,
    _behavior: DependencyType,
    _self_behavior: DependencyType,
    _reverse_self: bool,
) {
    unimplemented!()
}

pub fn new_object_addresses() -> ObjectAddresses {
    unimplemented!()
}

pub fn add_exact_object_address(_object: &ObjectAddress, _addrs: &mut ObjectAddresses) {
    unimplemented!()
}

pub fn object_address_present(_object: &ObjectAddress, _addrs: &ObjectAddresses) -> bool {
    unimplemented!()
}

pub fn record_object_address_dependencies(
    _depender: &ObjectAddress,
    _referenced: &mut ObjectAddresses,
    _behavior: DependencyType,
) {
    unimplemented!()
}

pub fn sort_object_addresses(_addrs: &mut ObjectAddresses) {
    unimplemented!()
}

pub fn free_object_addresses(_addrs: ObjectAddresses) {
    unimplemented!()
}

// in pg_depend.c
pub fn recordDependencyOn(
    _depender: &ObjectAddress,
    _referenced: &ObjectAddress,
    _behavior: DependencyType,
) {
    unimplemented!()
}

pub fn recordMultipleDependencies(
    _depender: &ObjectAddress,
    _referenced: &[ObjectAddress],
    _behavior: DependencyType,
) {
    unimplemented!()
}

pub fn recordDependencyOnCurrentExtension(_object: &ObjectAddress, _is_replace: bool) {
    unimplemented!()
}

pub fn checkMembershipInCurrentExtension(_object: &ObjectAddress) {
    unimplemented!()
}

pub fn deleteDependencyRecordsFor(_class_id: Oid, _object_id: Oid, _skip_extension_deps: bool) -> i64 {
    unimplemented!()
}

pub fn deleteDependencyRecordsForClass(
    _class_id: Oid,
    _object_id: Oid,
    _refclass_id: Oid,
    _deptype: i8,
) -> i64 {
    unimplemented!()
}

pub fn deleteDependencyRecordsForSpecific(
    _class_id: Oid,
    _object_id: Oid,
    _deptype: i8,
    _refclass_id: Oid,
    _refobject_id: Oid,
) -> i64 {
    unimplemented!()
}

pub fn changeDependencyFor(
    _class_id: Oid,
    _object_id: Oid,
    _ref_class_id: Oid,
    _old_ref_object_id: Oid,
    _new_ref_object_id: Oid,
) -> i64 {
    unimplemented!()
}

pub fn changeDependenciesOf(_class_id: Oid, _old_object_id: Oid, _new_object_id: Oid) -> i64 {
    unimplemented!()
}

pub fn changeDependenciesOn(_ref_class_id: Oid, _old_ref_object_id: Oid, _new_ref_object_id: Oid) -> i64 {
    unimplemented!()
}

// InvalidOid sentinel -> Option
pub fn getExtensionOfObject(_class_id: Oid, _object_id: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn getAutoExtensionsOfObject(_class_id: Oid, _object_id: Oid) -> List {
    unimplemented!()
}

pub fn getExtensionType(_extension_oid: Oid, _typname: &str) -> Option<Oid> {
    unimplemented!()
}

// returns bool + table/col out-params -> Option of the outputs
pub fn sequenceIsOwned(_seq_id: Oid, _deptype: i8) -> Option<(Oid, i32)> {
    unimplemented!()
}

pub fn getOwnedSequences(_relid: Oid) -> List {
    unimplemented!()
}

pub fn getIdentitySequence(_rel: &Relation, _attnum: AttrNumber, _missing_ok: bool) -> Option<Oid> {
    unimplemented!()
}

pub fn get_index_constraint(_index_id: Oid) -> Option<Oid> {
    unimplemented!()
}

pub fn get_index_ref_constraints(_index_id: Oid) -> List {
    unimplemented!()
}

// in pg_shdepend.c
pub fn recordSharedDependencyOn(
    _depender: &ObjectAddress,
    _referenced: &ObjectAddress,
    _deptype: SharedDependencyType,
) {
    unimplemented!()
}

pub fn deleteSharedDependencyRecordsFor(_class_id: Oid, _object_id: Oid, _object_sub_id: i32) {
    unimplemented!()
}

pub fn recordDependencyOnOwner(_class_id: Oid, _object_id: Oid, _owner: Oid) {
    unimplemented!()
}

pub fn changeDependencyOnOwner(_class_id: Oid, _object_id: Oid, _new_owner_id: Oid) {
    unimplemented!()
}

pub fn recordDependencyOnTablespace(_class_id: Oid, _object_id: Oid, _tablespace: Oid) {
    unimplemented!()
}

pub fn changeDependencyOnTablespace(_class_id: Oid, _object_id: Oid, _new_tablespace_id: Oid) {
    unimplemented!()
}

pub fn updateAclDependencies(
    _class_id: Oid,
    _object_id: Oid,
    _objsub_id: i32,
    _owner_id: Oid,
    _oldmembers: &[Oid],
    _newmembers: &[Oid],
) {
    unimplemented!()
}

pub fn updateInitAclDependencies(
    _class_id: Oid,
    _object_id: Oid,
    _objsub_id: i32,
    _oldmembers: &[Oid],
    _newmembers: &[Oid],
) {
    unimplemented!()
}

// returns bool + detail-message out-params
pub fn checkSharedDependencies(_class_id: Oid, _object_id: Oid) -> Option<(String, String)> {
    unimplemented!()
}

pub fn shdepLockAndCheckObject(_class_id: Oid, _object_id: Oid) {
    unimplemented!()
}

pub fn copyTemplateDependencies(_template_db_id: Oid, _new_db_id: Oid) {
    unimplemented!()
}

pub fn dropDatabaseDependencies(_database_id: Oid) {
    unimplemented!()
}

pub fn shdepDropOwned(_roleids: &List, _behavior: DropBehavior) {
    unimplemented!()
}

pub fn shdepReassignOwned(_roleids: &List, _newrole: Oid) {
    unimplemented!()
}
