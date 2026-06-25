//! Translated from PostgreSQL src/include/utils/acl.h
//!
//! Access control list data structures. An Acl is a varlena array of AclItem
//! (ON-DISK; size is hardcoded in pg_type.h for the aclitem type).

use crate::access::htup::HeapTuple;
use crate::nodes::parsenodes::{
    AclMode, AlterDefaultPrivilegesStmt, DropBehavior, GrantStmt, ObjectType, RoleSpec,
};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::utils::array::ArrayType;
use crate::utils::snapshot::Snapshot;

/// placeholder for id in a PUBLIC acl item
pub const ACL_ID_PUBLIC: Oid = Oid(0);

/// AclItem - one ACL entry. ON-DISK: array element of the `aclitem` type, so
/// layout is fixed (12 bytes: two Oid + one AclMode/u64).
///
/// Note: must be same size on all platforms (size hardcoded in pg_type.h).
/// The upper 32 bits of `ai_privs` are grant-option bits; the lower 32 bits are
/// the actual privileges. Use the ACLITEM_* accessors below.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AclItem {
    /// ID that this item grants privs to
    pub ai_grantee: Oid,
    /// grantor of privs
    pub ai_grantor: Oid,
    /// privilege bits (upper 32 = grant options, lower 32 = privileges)
    pub ai_privs: AclMode,
}
const _: () = assert!(core::mem::size_of::<AclItem>() == 16);

/// `ACLITEM_GET_PRIVS` - lower 32 privilege bits.
pub const fn aclitem_get_privs(item: AclItem) -> AclMode {
    AclMode::from_bits_retain(item.ai_privs.bits() & 0xFFFFFFFF)
}

/// `ACLITEM_GET_GOPTIONS` - upper 32 grant-option bits, shifted down.
pub const fn aclitem_get_goptions(item: AclItem) -> AclMode {
    AclMode::from_bits_retain((item.ai_privs.bits() >> 32) & 0xFFFFFFFF)
}

/// `ACLITEM_GET_RIGHTS` - combined grant-option + privilege bits.
pub const fn aclitem_get_rights(item: AclItem) -> AclMode {
    item.ai_privs
}

/// `ACL_GRANT_OPTION_FOR` - shift privs into the grant-option field.
pub const fn acl_grant_option_for(privs: AclMode) -> AclMode {
    AclMode::from_bits_retain((privs.bits() & 0xFFFFFFFF) << 32)
}

/// `ACL_OPTION_TO_PRIVS` - shift grant-option field down into privs.
pub const fn acl_option_to_privs(privs: AclMode) -> AclMode {
    AclMode::from_bits_retain((privs.bits() >> 32) & 0xFFFFFFFF)
}

/// `ACLITEM_SET_PRIVS` - replace the lower 32 privilege bits.
pub fn aclitem_set_privs(item: &mut AclItem, privs: AclMode) {
    let bits = (item.ai_privs.bits() & !0xFFFFFFFFu64) | (privs.bits() & 0xFFFFFFFF);
    item.ai_privs = AclMode::from_bits_retain(bits);
}

/// `ACLITEM_SET_GOPTIONS` - replace the upper 32 grant-option bits.
pub fn aclitem_set_goptions(item: &mut AclItem, goptions: AclMode) {
    let bits = (item.ai_privs.bits() & !(0xFFFFFFFFu64 << 32)) | ((goptions.bits() & 0xFFFFFFFF) << 32);
    item.ai_privs = AclMode::from_bits_retain(bits);
}

/// `ACLITEM_SET_RIGHTS` - replace both fields at once.
pub fn aclitem_set_rights(item: &mut AclItem, rights: AclMode) {
    item.ai_privs = rights;
}

/// `ACLITEM_SET_PRIVS_GOPTIONS` - set privs and grant options together.
pub fn aclitem_set_privs_goptions(item: &mut AclItem, privs: AclMode, goptions: AclMode) {
    let bits = (privs.bits() & 0xFFFFFFFF) | ((goptions.bits() & 0xFFFFFFFF) << 32);
    item.ai_privs = AclMode::from_bits_retain(bits);
}

/// `ACLITEM_ALL_PRIV_BITS` - all lower 32 privilege bits.
pub const ACLITEM_ALL_PRIV_BITS: AclMode = AclMode::from_bits_retain(0xFFFFFFFF);
/// `ACLITEM_ALL_GOPTION_BITS` - all upper 32 grant-option bits.
pub const ACLITEM_ALL_GOPTION_BITS: AclMode = AclMode::from_bits_retain(0xFFFFFFFFu64 << 32);

/// Acl - a one-dimensional array of AclItem (a standard PG varlena array type,
/// `ArrayType`). ON-DISK; toastable. Kept as a typedef over `ArrayType`.
pub type Acl = ArrayType;

/// ACL modification opcodes for aclupdate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclModeChg {
    Add = 1,
    Del = 2,
    Eql = 3,
}

/// External representation chars for each privilege bit (aclitemin/aclitemout).
pub const ACL_INSERT_CHR: u8 = b'a';
pub const ACL_SELECT_CHR: u8 = b'r';
pub const ACL_UPDATE_CHR: u8 = b'w';
pub const ACL_DELETE_CHR: u8 = b'd';
pub const ACL_TRUNCATE_CHR: u8 = b'D';
pub const ACL_REFERENCES_CHR: u8 = b'x';
pub const ACL_TRIGGER_CHR: u8 = b't';
pub const ACL_EXECUTE_CHR: u8 = b'X';
pub const ACL_USAGE_CHR: u8 = b'U';
pub const ACL_CREATE_CHR: u8 = b'C';
pub const ACL_CREATE_TEMP_CHR: u8 = b'T';
pub const ACL_CONNECT_CHR: u8 = b'c';
pub const ACL_SET_CHR: u8 = b's';
pub const ACL_ALTER_SYSTEM_CHR: u8 = b'A';
pub const ACL_MAINTAIN_CHR: u8 = b'm';

/// string holding all privilege code chars, in bitmask order
pub const ACL_ALL_RIGHTS_STR: &str = "arwdDxtXUCTcsAm";

/// "all rights" composite masks per object type.
pub const ACL_ALL_RIGHTS_COLUMN: AclMode =
    AclMode::from_bits_retain(AclMode::INSERT.bits() | AclMode::SELECT.bits() | AclMode::UPDATE.bits() | AclMode::REFERENCES.bits());
pub const ACL_ALL_RIGHTS_RELATION: AclMode = AclMode::from_bits_retain(
    AclMode::INSERT.bits() | AclMode::SELECT.bits() | AclMode::UPDATE.bits() | AclMode::DELETE.bits()
        | AclMode::TRUNCATE.bits() | AclMode::REFERENCES.bits() | AclMode::TRIGGER.bits() | AclMode::MAINTAIN.bits(),
);
pub const ACL_ALL_RIGHTS_SEQUENCE: AclMode =
    AclMode::from_bits_retain(AclMode::USAGE.bits() | AclMode::SELECT.bits() | AclMode::UPDATE.bits());
pub const ACL_ALL_RIGHTS_DATABASE: AclMode =
    AclMode::from_bits_retain(AclMode::CREATE.bits() | AclMode::CREATE_TEMP.bits() | AclMode::CONNECT.bits());
pub const ACL_ALL_RIGHTS_FDW: AclMode = AclMode::USAGE;
pub const ACL_ALL_RIGHTS_FOREIGN_SERVER: AclMode = AclMode::USAGE;
pub const ACL_ALL_RIGHTS_FUNCTION: AclMode = AclMode::EXECUTE;
pub const ACL_ALL_RIGHTS_LANGUAGE: AclMode = AclMode::USAGE;
pub const ACL_ALL_RIGHTS_LARGEOBJECT: AclMode =
    AclMode::from_bits_retain(AclMode::SELECT.bits() | AclMode::UPDATE.bits());
pub const ACL_ALL_RIGHTS_PARAMETER_ACL: AclMode =
    AclMode::from_bits_retain(AclMode::SET.bits() | AclMode::ALTER_SYSTEM.bits());
pub const ACL_ALL_RIGHTS_SCHEMA: AclMode =
    AclMode::from_bits_retain(AclMode::USAGE.bits() | AclMode::CREATE.bits());
pub const ACL_ALL_RIGHTS_TABLESPACE: AclMode = AclMode::CREATE;
pub const ACL_ALL_RIGHTS_TYPE: AclMode = AclMode::USAGE;

/// operation codes for pg_*_aclmask
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclMaskHow {
    /// normal case: compute all bits
    All,
    /// return when result is known nonzero
    Any,
}

/// result codes for pg_*_aclcheck
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclResult {
    Ok = 0,
    NoPriv,
    NotOwner,
}

pub fn acldefault(_objtype: ObjectType, _owner_id: Oid) -> *mut Acl {
    unimplemented!()
}

pub fn get_user_default_acl(_objtype: ObjectType, _owner_id: Oid, _nsp_oid: Oid) -> *mut Acl {
    unimplemented!()
}

pub fn record_dependency_on_new_acl(_class_id: Oid, _object_id: Oid, _objsub_id: i32, _owner_id: Oid, _acl: &Acl) {
    unimplemented!()
}

pub fn aclupdate(_old_acl: &Acl, _mod_aip: &AclItem, _modechg: AclModeChg, _owner_id: Oid, _behavior: DropBehavior) -> *mut Acl {
    unimplemented!()
}

pub fn aclnewowner(_old_acl: &Acl, _old_owner_id: Oid, _new_owner_id: Oid) -> *mut Acl {
    unimplemented!()
}

pub fn make_empty_acl() -> *mut Acl {
    unimplemented!()
}

pub fn aclcopy(_orig_acl: &Acl) -> *mut Acl {
    unimplemented!()
}

pub fn aclconcat(_left_acl: &Acl, _right_acl: &Acl) -> *mut Acl {
    unimplemented!()
}

pub fn aclmerge(_left_acl: &Acl, _right_acl: &Acl, _owner_id: Oid) -> *mut Acl {
    unimplemented!()
}

pub fn aclitemsort(_acl: &mut Acl) {
    unimplemented!()
}

pub fn aclequal(_left_acl: &Acl, _right_acl: &Acl) -> bool {
    unimplemented!()
}

pub fn aclmask(_acl: &Acl, _roleid: Oid, _owner_id: Oid, _mask: AclMode, _how: AclMaskHow) -> AclMode {
    unimplemented!()
}

/// `aclmembers` - C returned count + `Oid **roleids` out-param; map to the Vec.
pub fn aclmembers(_acl: &Acl) -> Vec<Oid> {
    unimplemented!()
}

pub fn has_privs_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!()
}

pub fn member_can_set_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!()
}

pub fn check_can_set_role(_member: Oid, _role: Oid) {
    unimplemented!()
}

pub fn is_member_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!()
}

pub fn is_member_of_role_nosuper(_member: Oid, _role: Oid) -> bool {
    unimplemented!()
}

pub fn is_admin_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!()
}

/// `select_best_admin` - InvalidOid sentinel -> Option.
pub fn select_best_admin(_member: Oid, _role: Oid) -> Option<Oid> {
    unimplemented!()
}

/// `get_role_oid` - drops `missing_ok`; None when not found.
pub fn get_role_oid(_rolname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn get_role_oid_or_public(_rolname: &str) -> Oid {
    unimplemented!()
}

/// `get_rolespec_oid` - drops `missing_ok`; None when not found.
pub fn get_rolespec_oid(_role: &RoleSpec) -> Option<Oid> {
    unimplemented!()
}

pub fn check_rolespec_name(_role: &RoleSpec, _detail_msg: &str) {
    unimplemented!()
}

pub fn get_rolespec_tuple(_role: &RoleSpec) -> HeapTuple {
    unimplemented!()
}

pub fn get_rolespec_name(_role: &RoleSpec) -> String {
    unimplemented!()
}

/// `select_best_grantor` - two out-params `grantorId`/`grantOptions` -> tuple.
pub fn select_best_grantor(_role_id: Oid, _privileges: AclMode, _acl: &Acl, _owner_id: Oid) -> (Oid, AclMode) {
    unimplemented!()
}

pub fn initialize_acl() {
    unimplemented!()
}

pub fn execute_grant_stmt(_stmt: &GrantStmt) {
    unimplemented!()
}

pub fn exec_alter_default_privileges_stmt(_pstate: &mut ParseState, _stmt: &AlterDefaultPrivilegesStmt) {
    unimplemented!()
}

pub fn remove_role_from_object_acl(_roleid: Oid, _classid: Oid, _objid: Oid) {
    unimplemented!()
}

pub fn pg_class_aclmask(_table_oid: Oid, _roleid: Oid, _mask: AclMode, _how: AclMaskHow) -> AclMode {
    unimplemented!()
}

pub fn object_aclcheck(_classid: Oid, _objectid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult {
    unimplemented!()
}

/// `object_aclcheck_ext` - `bool *is_missing` out-param -> tuple.
pub fn object_aclcheck_ext(_classid: Oid, _objectid: Oid, _roleid: Oid, _mode: AclMode) -> (AclResult, bool) {
    unimplemented!()
}

pub fn pg_attribute_aclcheck(_table_oid: Oid, _attnum: i16, _roleid: Oid, _mode: AclMode) -> AclResult {
    unimplemented!()
}

pub fn pg_attribute_aclcheck_ext(_table_oid: Oid, _attnum: i16, _roleid: Oid, _mode: AclMode) -> (AclResult, bool) {
    unimplemented!()
}

pub fn pg_attribute_aclcheck_all(_table_oid: Oid, _roleid: Oid, _mode: AclMode, _how: AclMaskHow) -> AclResult {
    unimplemented!()
}

pub fn pg_attribute_aclcheck_all_ext(_table_oid: Oid, _roleid: Oid, _mode: AclMode, _how: AclMaskHow) -> (AclResult, bool) {
    unimplemented!()
}

pub fn pg_class_aclcheck(_table_oid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult {
    unimplemented!()
}

pub fn pg_class_aclcheck_ext(_table_oid: Oid, _roleid: Oid, _mode: AclMode) -> (AclResult, bool) {
    unimplemented!()
}

pub fn pg_parameter_aclcheck(_name: &str, _roleid: Oid, _mode: AclMode) -> AclResult {
    unimplemented!()
}

pub fn pg_largeobject_aclcheck_snapshot(_lobj_oid: Oid, _roleid: Oid, _mode: AclMode, _snapshot: Snapshot<'_>) -> AclResult {
    unimplemented!()
}

pub fn aclcheck_error(_aclerr: AclResult, _objtype: ObjectType, _objectname: &str) {
    unimplemented!()
}

pub fn aclcheck_error_col(_aclerr: AclResult, _objtype: ObjectType, _objectname: &str, _colname: &str) {
    unimplemented!()
}

pub fn aclcheck_error_type(_aclerr: AclResult, _type_oid: Oid) {
    unimplemented!()
}

pub fn record_ext_obj_init_priv(_objoid: Oid, _classoid: Oid) {
    unimplemented!()
}

pub fn remove_ext_obj_init_priv(_objoid: Oid, _classoid: Oid) {
    unimplemented!()
}

pub fn replace_role_in_init_priv(_oldroleid: Oid, _newroleid: Oid, _classid: Oid, _objid: Oid, _objsubid: i32) {
    unimplemented!()
}

pub fn remove_role_from_init_priv(_roleid: Oid, _classid: Oid, _objid: Oid, _objsubid: i32) {
    unimplemented!()
}

pub fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    unimplemented!()
}

pub fn has_createrole_privilege(_roleid: Oid) -> bool {
    unimplemented!()
}

pub fn has_bypassrls_privilege(_roleid: Oid) -> bool {
    unimplemented!()
}
