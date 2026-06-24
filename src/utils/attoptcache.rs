//! Translated from PostgreSQL src/include/utils/attoptcache.h

use crate::postgres_ext::Oid;

/// Attribute options. On-disk varlena prefix (vl_len_) kept for layout.
#[repr(C)]
pub struct AttributeOpts {
    pub vl_len_: i32, // varlena header (do not touch directly!)
    pub n_distinct: f64,
    pub n_distinct_inherited: f64,
}

pub fn get_attribute_options(attrelid: Oid, attnum: i32) -> Option<Box<AttributeOpts>> {
    let _ = (attrelid, attnum);
    unimplemented!()
}
