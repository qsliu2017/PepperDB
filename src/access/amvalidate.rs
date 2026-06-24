//! Translated from PostgreSQL src/include/access/amvalidate.h

use crate::postgres_ext::Oid;
use crate::utils::catcache::CatCList;

/// Struct returned (in a list) by identify_opfamily_groups().
pub struct OpFamilyOpFuncGroup {
    pub lefttype: Oid,    // amoplefttype/amproclefttype
    pub righttype: Oid,   // amoprighttype/amprocrighttype
    pub operatorset: u64, // bitmask of operators with these types
    pub functionset: u64, // bitmask of support funcs with these types
}

// Functions in access/index/amvalidate.c
// C returns `List *`; pg_list is a tombstone, so use Vec.
pub fn identify_opfamily_groups(
    _oprlist: &CatCList,
    _proclist: &CatCList,
) -> Vec<OpFamilyOpFuncGroup> {
    unimplemented!()
}

// C uses trailing varargs (a type-oid list); model as a slice.
pub fn check_amproc_signature(
    _funcid: Oid,
    _restype: Oid,
    _exact: bool,
    _minargs: i32,
    _maxargs: i32,
    _argtypes: &[Oid],
) -> bool {
    unimplemented!()
}

pub fn check_amoptsproc_signature(_funcid: Oid) -> bool {
    unimplemented!()
}

pub fn check_amop_signature(_opno: Oid, _restype: Oid, _lefttype: Oid, _righttype: Oid) -> bool {
    unimplemented!()
}

/// None when no opclass found (C returns InvalidOid as the sentinel).
pub fn opclass_for_family_datatype(
    _amoid: Oid,
    _opfamilyoid: Oid,
    _datatypeoid: Oid,
) -> Option<Oid> {
    unimplemented!()
}

pub fn opfamily_can_sort_type(_opfamilyoid: Oid, _datatypeoid: Oid) -> bool {
    unimplemented!()
}
