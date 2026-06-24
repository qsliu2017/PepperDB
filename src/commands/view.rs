//! Translated from PostgreSQL src/include/commands/view.h

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::{Query, ViewStmt};
use crate::postgres_ext::Oid;

pub fn DefineView(
    stmt: &mut ViewStmt,
    queryString: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> ObjectAddress {
    unimplemented!()
}

pub fn StoreViewQuery(viewOid: Oid, viewParse: &mut Query, replace: bool) {
    unimplemented!()
}
