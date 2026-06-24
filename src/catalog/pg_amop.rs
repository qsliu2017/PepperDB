//! Translated from PostgreSQL src/include/catalog/pg_amop.h

use crate::postgres_ext::Oid;

pub const AccessMethodOperatorRelationId: Oid = Oid(2602);

#[repr(C)]
pub struct FormData_pg_amop {
    pub oid: Oid,
    pub amopfamily: Oid,    // BKI_LOOKUP(pg_opfamily)
    pub amoplefttype: Oid,  // BKI_LOOKUP(pg_type)
    pub amoprighttype: Oid, // BKI_LOOKUP(pg_type)
    pub amopstrategy: i16,
    pub amoppurpose: i8,
    pub amopopr: Oid,        // BKI_LOOKUP(pg_operator)
    pub amopmethod: Oid,     // BKI_LOOKUP(pg_am)
    pub amopsortfamily: Oid, // BKI_LOOKUP_OPT(pg_opfamily)
}

pub type Form_pg_amop = *mut FormData_pg_amop; // TODO(ptr)

// DECLARE_UNIQUE_INDEX(pg_amop_fam_strat_index, 2653, AccessMethodStrategyIndexId)
// DECLARE_UNIQUE_INDEX(pg_amop_opr_fam_index, 2654, AccessMethodOperatorIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_amop_oid_index, 2756, AccessMethodOperatorOidIndexId)
// MAKE_SYSCACHE(AMOPSTRATEGY, pg_amop_fam_strat_index, 64)
// MAKE_SYSCACHE(AMOPOPID, pg_amop_opr_fam_index, 64)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_amop_oid: i32 = 1;
pub const Anum_pg_amop_amopfamily: i32 = 2;
pub const Anum_pg_amop_amoplefttype: i32 = 3;
pub const Anum_pg_amop_amoprighttype: i32 = 4;
pub const Anum_pg_amop_amopstrategy: i32 = 5;
pub const Anum_pg_amop_amoppurpose: i32 = 6;
pub const Anum_pg_amop_amopopr: i32 = 7;
pub const Anum_pg_amop_amopmethod: i32 = 8;
pub const Anum_pg_amop_amopsortfamily: i32 = 9;
pub const Natts_pg_amop: i32 = 9;

// allowed values of amoppurpose
pub const AMOP_SEARCH: i8 = b's' as i8;
pub const AMOP_ORDER: i8 = b'o' as i8;
