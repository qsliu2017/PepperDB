//! Translation of postgres/src/include/catalog/pg_class.h
//!
//! The `FormData_pg_class` struct: the fixed-layout part of a pg_class catalog
//! row.  As in the C header, the struct as compiled into the backend stops at
//! the field just before `#ifdef CATALOG_VARLEN`; the trailing variable-length
//! fields (relacl[], reloptions[], relpartbound, guarded by CATALOG_VARLEN) are
//! NOT part of this in-memory struct - they live only in a real on-disk pg_class
//! tuple and are reached via heap_getattr.  These are also the fields absent
//! from a relcache entry's rd_rel.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{float4, int16, int32, MultiXactId, NameData, TransactionId};
use crate::postgres_ext::Oid;
use crate::prelude::*;
use crate::utils::error::elog_impl::errdetail_c;
use core::ffi::{c_char, c_int};

/*
 * FormData_pg_class - the fixed part of a pg_class row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; the
 * relcache copies this fixed part verbatim into rd_rel and downstream code
 * computes offsets/sizes from it.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_class {
    /* oid */
    pub oid: Oid,
    /* class name */
    pub relname: NameData,
    /* OID of namespace containing this class */
    pub relnamespace: Oid,
    /* OID of entry in pg_type for relation's implicit row type, if any */
    pub reltype: Oid,
    /* OID of entry in pg_type for underlying composite type, if any */
    pub reloftype: Oid,
    /* class owner */
    pub relowner: Oid,
    /* access method; 0 if not a table / index */
    pub relam: Oid,
    /* identifier of physical storage file; 0 means a "mapped" relation */
    pub relfilenode: Oid,
    /* identifier of table space for relation (0 means default for database) */
    pub reltablespace: Oid,
    /* # of blocks (not always up-to-date) */
    pub relpages: int32,
    /* # of tuples (not always up-to-date; -1 means "unknown") */
    pub reltuples: float4,
    /* # of all-visible blocks (not always up-to-date) */
    pub relallvisible: int32,
    /* # of all-frozen blocks (not always up-to-date) */
    pub relallfrozen: int32,
    /* OID of toast table; 0 if none */
    pub reltoastrelid: Oid,
    /* T if has (or has had) any indexes */
    pub relhasindex: bool,
    /* T if shared across databases */
    pub relisshared: bool,
    /* see RELPERSISTENCE_xxx constants below */
    pub relpersistence: c_char,
    /* see RELKIND_xxx constants below */
    pub relkind: c_char,
    /* number of user attributes */
    pub relnatts: int16,
    /* # of CHECK constraints for class */
    pub relchecks: int16,
    /* has (or has had) any rules */
    pub relhasrules: bool,
    /* has (or has had) any TRIGGERs */
    pub relhastriggers: bool,
    /* has (or has had) child tables or indexes */
    pub relhassubclass: bool,
    /* row security is enabled or not */
    pub relrowsecurity: bool,
    /* row security forced for owners or not */
    pub relforcerowsecurity: bool,
    /* matview currently holds query results */
    pub relispopulated: bool,
    /* see REPLICA_IDENTITY_xxx constants */
    pub relreplident: c_char,
    /* is relation a partition? */
    pub relispartition: bool,
    /* link to original rel during table rewrite; otherwise 0 */
    pub relrewrite: Oid,
    /* all Xids < this are frozen in this rel */
    pub relfrozenxid: TransactionId,
    /* all multixacts in this rel are >= this; it is really a MultiXactId */
    pub relminmxid: MultiXactId,
}

/*
 * Form_pg_class corresponds to a pointer to a tuple with the format of the
 * pg_class relation.
 */
pub type Form_pg_class = *mut FormData_pg_class;

/*
 * CLASS_TUPLE_SIZE is the size of the fixed part of pg_class tuples, not
 * counting var-length fields.
 *
 *   #define CLASS_TUPLE_SIZE \
 *       (offsetof(FormData_pg_class,relminmxid) + sizeof(TransactionId))
 */
pub const CLASS_TUPLE_SIZE: usize =
    core::mem::offset_of!(FormData_pg_class, relminmxid) + core::mem::size_of::<TransactionId>();

/* RELKIND_xxx (EXPOSE_TO_CLIENT_CODE) */
pub const RELKIND_RELATION: c_char = b'r' as c_char; /* ordinary table */
pub const RELKIND_INDEX: c_char = b'i' as c_char; /* secondary index */
pub const RELKIND_SEQUENCE: c_char = b'S' as c_char; /* sequence object */
pub const RELKIND_TOASTVALUE: c_char = b't' as c_char; /* for out-of-line values */
pub const RELKIND_VIEW: c_char = b'v' as c_char; /* view */
pub const RELKIND_MATVIEW: c_char = b'm' as c_char; /* materialized view */
pub const RELKIND_COMPOSITE_TYPE: c_char = b'c' as c_char; /* composite type */
pub const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char; /* foreign table */
pub const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char; /* partitioned table */
pub const RELKIND_PARTITIONED_INDEX: c_char = b'I' as c_char; /* partitioned index */

/* RELPERSISTENCE_xxx (EXPOSE_TO_CLIENT_CODE) */
pub const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char; /* regular table */
pub const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char; /* unlogged permanent table */
pub const RELPERSISTENCE_TEMP: c_char = b't' as c_char; /* temporary table */

/* REPLICA_IDENTITY_xxx (EXPOSE_TO_CLIENT_CODE) */
/* default selection for replica identity (primary key or nothing) */
pub const REPLICA_IDENTITY_DEFAULT: c_char = b'd' as c_char;
/* no replica identity is logged for this relation */
pub const REPLICA_IDENTITY_NOTHING: c_char = b'n' as c_char;
/* all columns are logged as replica identity */
pub const REPLICA_IDENTITY_FULL: c_char = b'f' as c_char;
/* an explicitly chosen candidate key's columns are used as replica identity */
pub const REPLICA_IDENTITY_INDEX: c_char = b'i' as c_char;

/*
 * Issue an errdetail() informing that the relkind is not supported for this
 * operation.
 */
pub unsafe fn errdetail_relkind_not_supported(relkind: c_char) -> c_int {
    match relkind {
        RELKIND_RELATION => errdetail_c(c"This operation is not supported for tables.".as_ptr()),
        RELKIND_INDEX => errdetail_c(c"This operation is not supported for indexes.".as_ptr()),
        RELKIND_SEQUENCE => {
            errdetail_c(c"This operation is not supported for sequences.".as_ptr())
        }
        RELKIND_TOASTVALUE => {
            errdetail_c(c"This operation is not supported for TOAST tables.".as_ptr())
        }
        RELKIND_VIEW => errdetail_c(c"This operation is not supported for views.".as_ptr()),
        RELKIND_MATVIEW => {
            errdetail_c(c"This operation is not supported for materialized views.".as_ptr())
        }
        RELKIND_COMPOSITE_TYPE => {
            errdetail_c(c"This operation is not supported for composite types.".as_ptr())
        }
        RELKIND_FOREIGN_TABLE => {
            errdetail_c(c"This operation is not supported for foreign tables.".as_ptr())
        }
        RELKIND_PARTITIONED_TABLE => {
            errdetail_c(c"This operation is not supported for partitioned tables.".as_ptr())
        }
        RELKIND_PARTITIONED_INDEX => {
            errdetail_c(c"This operation is not supported for partitioned indexes.".as_ptr())
        }
        _ => {
            elog!(ERROR, "unrecognized relkind: '{}'", relkind as u8 as char);
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // relname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_class, relname), 4);
        // relminmxid is the last fixed field; CLASS_TUPLE_SIZE ends right after
        // it (TransactionId == MultiXactId == uint32, 4 bytes).
        assert_eq!(
            CLASS_TUPLE_SIZE,
            core::mem::offset_of!(FormData_pg_class, relminmxid)
                + core::mem::size_of::<TransactionId>()
        );
        // The struct must be at least as large as its fixed part (alignment may
        // add trailing padding, which is fine - the C struct has it too).
        assert!(core::mem::size_of::<FormData_pg_class>() >= CLASS_TUPLE_SIZE);
    }
}
