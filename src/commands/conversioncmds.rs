//! Translated from PostgreSQL src/include/commands/conversioncmds.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::CreateConversionStmt;

pub fn CreateConversionCommand(_stmt: &CreateConversionStmt) -> ObjectAddress {
    unimplemented!()
}
