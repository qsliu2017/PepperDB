//! Translated from PostgreSQL src/include/commands/comment.h
//!
//! Public functions of the comment routines. CommentObject() implements the SQL
//! "COMMENT ON" command. DeleteComments() deletes all comments for an object.
//! CreateComments creates (or deletes, if comment is None) a comment for a
//! specific key. There are versions of these two methods for both normal and
//! shared objects.

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::parsenodes::CommentStmt;
use crate::postgres_ext::Oid;

pub fn CommentObject(_stmt: &CommentStmt) -> ObjectAddress {
    unimplemented!()
}

pub fn DeleteComments(_oid: Oid, _classoid: Oid, _subid: i32) {
    unimplemented!()
}

pub fn CreateComments(_oid: Oid, _classoid: Oid, _subid: i32, _comment: Option<&str>) {
    unimplemented!()
}

pub fn DeleteSharedComments(_oid: Oid, _classoid: Oid) {
    unimplemented!()
}

pub fn CreateSharedComments(_oid: Oid, _classoid: Oid, _comment: Option<&str>) {
    unimplemented!()
}

pub fn GetComment(_oid: Oid, _classoid: Oid, _subid: i32) -> Option<String> {
    unimplemented!()
}
