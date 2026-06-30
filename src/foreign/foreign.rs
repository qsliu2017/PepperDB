//! Translated from PostgreSQL src/include/foreign/foreign.h

use bitflags::bitflags;

use crate::access::attnum::AttrNumber;
use crate::nodes::nodes::Node;
use crate::postgres_ext::Oid;

bitflags! {
    /// Flags for GetForeignServerExtended. GOOD: single-bit set (C `bits16`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FsvFlags: u16 {
        const MISSING_OK = 0x01;
    }
}

bitflags! {
    /// Flags for GetForeignDataWrapperExtended. GOOD: single-bit set (`bits16`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FdwFlags: u16 {
        const MISSING_OK = 0x01;
    }
}

/// In-memory catalog cache entry for a foreign-data wrapper. `options` is the
/// fdwoptions DefElem list (List* -> Vec).
pub struct ForeignDataWrapper {
    pub fdwid: Oid,             // FDW Oid
    pub owner: Oid,            // FDW owner user Oid
    pub fdwname: String,       // Name of the FDW
    pub fdwhandler: Oid,       // Oid of handler function, or 0
    pub fdwvalidator: Oid,     // Oid of validator function, or 0
    pub options: Vec<Node>, // fdwoptions as DefElem list
}

pub struct ForeignServer {
    pub serverid: Oid,            // server Oid
    pub fdwid: Oid,               // foreign-data wrapper
    pub owner: Oid,               // server owner user Oid
    pub servername: String,       // name of the server
    pub servertype: Option<String>, // server type, optional
    pub serverversion: Option<String>, // server version, optional
    pub options: Vec<Node>,  // srvoptions as DefElem list
}

pub struct UserMapping {
    pub umid: Oid,               // Oid of user mapping
    pub userid: Oid,             // local user Oid
    pub serverid: Oid,           // server Oid
    pub options: Vec<Node>, // useoptions as DefElem list
}

pub struct ForeignTable {
    pub relid: Oid,              // relation Oid
    pub serverid: Oid,           // server Oid
    pub options: Vec<Node>, // ftoptions as DefElem list
}

/// MappingUserName(userid): username for a user mapping, "public" if invalid.
pub fn MappingUserName(userid: Oid) -> String {
    if userid == Oid::new(0) {
        "public".to_string()
    } else {
        GetUserNameFromId(userid, false)
    }
}

// Not-found lookups: invalid-Oid / missing_ok / FSV_/FDW_MISSING_OK collapse to
// Option (function-mapping section 4); the flag/missing_ok arg disappears.

pub fn GetForeignServer(_serverid: Oid) -> Option<ForeignServer> {
    unimplemented!()
}

pub fn GetForeignServerExtended(_serverid: Oid, _flags: FsvFlags) -> Option<ForeignServer> {
    unimplemented!()
}

pub fn GetForeignServerByName(_srvname: &str) -> Option<ForeignServer> {
    unimplemented!()
}

pub fn GetUserMapping(_userid: Oid, _serverid: Oid) -> Option<UserMapping> {
    unimplemented!()
}

pub fn GetForeignDataWrapper(_fdwid: Oid) -> Option<ForeignDataWrapper> {
    unimplemented!()
}

pub fn GetForeignDataWrapperExtended(
    _fdwid: Oid,
    _flags: FdwFlags,
) -> Option<ForeignDataWrapper> {
    unimplemented!()
}

pub fn GetForeignDataWrapperByName(_fdwname: &str) -> Option<ForeignDataWrapper> {
    unimplemented!()
}

pub fn GetForeignTable(_relid: Oid) -> Option<ForeignTable> {
    unimplemented!()
}

pub fn GetForeignColumnOptions(_relid: Oid, _attnum: AttrNumber) -> Vec<Node> {
    unimplemented!()
}

pub fn get_foreign_data_wrapper_oid(_fdwname: &str) -> Option<Oid> {
    unimplemented!()
}

pub fn get_foreign_server_oid(_servername: &str) -> Option<Oid> {
    unimplemented!()
}

// Local stand-in for miscadmin's GetUserNameFromId (canonical returns Option<String>;
// kept local to preserve MappingUserName's -> String body).
fn GetUserNameFromId(_userid: Oid, _noerr: bool) -> String {
    unimplemented!()
}
