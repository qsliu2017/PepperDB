//! Translated from PostgreSQL src/include/tcop/deparse_utility.h
//!
//! Support for keeping track of collected commands. The C `CollectedCommand`
//! is a tagged union (discriminant `CollectedCommandType`); it maps to a Rust
//! enum whose variants carry the per-type payload.

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::ObjectType;
use crate::postgres_ext::Oid;
use crate::utils::aclchk_internal::InternalGrant;

/// CollectedCommandType: the kind of collected command (the union discriminant).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectedCommandType {
    Simple,
    AlterTable,
    Grant,
    AlterOpFamily,
    AlterDefaultPrivileges,
    CreateOpClass,
    AlterTSConfig,
}

/// For ALTER TABLE commands, one subcommand therein.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectedATSubcmd {
    /// affected column, constraint, index, ...
    pub address: ObjectAddress,
    pub parsetree: Box<Node>,
}

/// Per-type payload for the ALTER TABLE variant.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectedAlterTable {
    pub object_id: Oid,
    pub class_id: Oid,
    pub subcmds: Vec<CollectedATSubcmd>,
}

/// CollectedCommand: a record of one executed command, with type-specific data.
///
/// The C `union d` is folded into the variant payloads. The shared fields
/// (`in_extension`, `parsetree`, `parent`) live on every variant via the outer
/// struct.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectedCommand {
    pub in_extension: bool,
    pub parsetree: Option<Box<Node>>,
    /// when nested
    pub parent: Option<Box<CollectedCommand>>,
    pub data: CollectedCommandData,
}

/// The tagged-union payload (`CollectedCommandType type` + `union d`).
#[derive(Debug, Clone, PartialEq)]
pub enum CollectedCommandData {
    /// most commands
    Simple {
        address: ObjectAddress,
        secondary_object: ObjectAddress,
    },
    /// ALTER TABLE, and internal uses thereof
    AlterTable(CollectedAlterTable),
    /// GRANT / REVOKE
    Grant { istmt: Box<InternalGrant> },
    /// ALTER OPERATOR FAMILY
    AlterOpFamily {
        address: ObjectAddress,
        operators: Vec<Box<Node>>,
        procedures: Vec<Box<Node>>,
    },
    /// CREATE OPERATOR CLASS
    CreateOpClass {
        address: ObjectAddress,
        operators: Vec<Box<Node>>,
        procedures: Vec<Box<Node>>,
    },
    /// ALTER TEXT SEARCH CONFIGURATION ADD/ALTER/DROP MAPPING
    AlterTSConfig {
        address: ObjectAddress,
        dict_ids: Vec<Oid>,
    },
    /// ALTER DEFAULT PRIVILEGES
    AlterDefaultPrivileges { objtype: ObjectType },
}
