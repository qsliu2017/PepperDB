//! tcop/deparse_utility.h - support for keeping track of collected commands.

use std::ffi::c_int;

use crate::c::int32;
use crate::postgres_ext::Oid;

// ObjectAddress currently lives as a stub in catalog::objectaccess (objectaddress.h
// not yet ported). TODO: dedup when catalog/objectaddress.h lands.
use crate::catalog::objectaccess::ObjectAddress;
// Node / List from the nodes subsystem.
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::List;
// ObjectType enum projection.
use crate::nodes::parsenodes::ObjectType;
// GRANT/REVOKE internal representation.
use crate::utils::aclchk_internal::InternalGrant;

/*
 * Support for keeping track of collected commands.
 */
// C enum CollectedCommandType -> c_int + consts (project convention).
pub type CollectedCommandType = c_int;
pub const SCT_Simple: CollectedCommandType = 0;
pub const SCT_AlterTable: CollectedCommandType = 1;
pub const SCT_Grant: CollectedCommandType = 2;
pub const SCT_AlterOpFamily: CollectedCommandType = 3;
pub const SCT_AlterDefaultPrivileges: CollectedCommandType = 4;
pub const SCT_CreateOpClass: CollectedCommandType = 5;
pub const SCT_AlterTSConfig: CollectedCommandType = 6;

/*
 * For ALTER TABLE commands, we keep a list of the subcommands therein.
 */
#[repr(C)]
pub struct CollectedATSubcmd {
    pub address: ObjectAddress, // affected column, constraint, index, ...
    pub parsetree: *mut Node,
}

// Named projections of the anonymous structs inside CollectedCommand's union `d`.

/* most commands */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommand_simple {
    pub address: ObjectAddress,
    pub secondaryObject: ObjectAddress,
}

/* ALTER TABLE, and internal uses thereof */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommand_alterTable {
    pub objectId: Oid,
    pub classId: Oid,
    pub subcmds: *mut List,
}

/* GRANT / REVOKE */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommand_grant {
    pub istmt: *mut InternalGrant,
}

/* ALTER OPERATOR FAMILY */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommand_opfam {
    pub address: ObjectAddress,
    pub operators: *mut List,
    pub procedures: *mut List,
}

/* CREATE OPERATOR CLASS */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommand_createopc {
    pub address: ObjectAddress,
    pub operators: *mut List,
    pub procedures: *mut List,
}

/* ALTER TEXT SEARCH CONFIGURATION ADD/ALTER/DROP MAPPING */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommand_atscfg {
    pub address: ObjectAddress,
    pub dictIds: *mut Oid,
    pub ndicts: c_int,
}

/* ALTER DEFAULT PRIVILEGES */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommand_defprivs {
    pub objtype: ObjectType,
}

// The anonymous union `d` inside CollectedCommand.
#[repr(C)]
pub union CollectedCommand_d {
    pub simple: CollectedCommand_simple,
    pub alterTable: CollectedCommand_alterTable,
    pub grant: CollectedCommand_grant,
    pub opfam: CollectedCommand_opfam,
    pub createopc: CollectedCommand_createopc,
    pub atscfg: CollectedCommand_atscfg,
    pub defprivs: CollectedCommand_defprivs,
}

#[repr(C)]
pub struct CollectedCommand {
    pub type_: CollectedCommandType,

    pub in_extension: bool,
    pub parsetree: *mut Node,

    pub d: CollectedCommand_d,

    pub parent: *mut CollectedCommand, // when nested
}
