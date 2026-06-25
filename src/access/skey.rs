//! Translated from PostgreSQL src/include/access/skey.h

use bitflags::bitflags;

use crate::access::attnum::AttrNumber;
use crate::access::stratnum::StrategyNumber;
use crate::c::RegProcedure;
use crate::fmgr::FmgrInfo;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

bitflags! {
    /// ScanKeyData flags. Bits 0-15 are system-wide (defined here); bits 16-31
    /// are reserved for individual index access methods.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ScanKeyFlags: i32 {
        const ISNULL       = 0x0001; // argument is NULL
        const UNARY        = 0x0002; // unary operator (not supported!)
        const ROW_HEADER   = 0x0004; // row comparison header
        const ROW_MEMBER   = 0x0008; // row comparison member
        const ROW_END      = 0x0010; // last row comparison member
        const SEARCHARRAY  = 0x0020; // scankey represents ScalarArrayOp
        const SEARCHNULL   = 0x0040; // scankey represents "col IS NULL"
        const SEARCHNOTNULL = 0x0080; // scankey represents "col IS NOT NULL"
        const ORDER_BY     = 0x0100; // scankey is for ORDER BY op
    }
}

/// Application of a comparison operator between a column and a constant.
pub struct ScanKeyData {
    pub flags: i32, // flags, see ScanKeyFlags
    pub attno: AttrNumber,
    pub strategy: StrategyNumber,
    pub subtype: Oid,
    pub collation: Oid,
    pub func: FmgrInfo,
    pub argument: Datum,
}

/// C `ScanKey` is `ScanKeyData *`; modeled as a borrow at call sites.
pub type ScanKey<'a> = &'a mut ScanKeyData;

pub fn ScanKeyInit(
    _entry: &mut ScanKeyData,
    _attribute_number: AttrNumber,
    _strategy: StrategyNumber,
    _procedure: RegProcedure,
    _argument: Datum,
) {
    unimplemented!()
}

pub fn ScanKeyEntryInitialize(
    _entry: &mut ScanKeyData,
    _flags: i32,
    _attribute_number: AttrNumber,
    _strategy: StrategyNumber,
    _subtype: Oid,
    _collation: Oid,
    _procedure: RegProcedure,
    _argument: Datum,
) {
    unimplemented!()
}

pub fn ScanKeyEntryInitializeWithInfo(
    _entry: &mut ScanKeyData,
    _flags: i32,
    _attribute_number: AttrNumber,
    _strategy: StrategyNumber,
    _subtype: Oid,
    _collation: Oid,
    _finfo: &FmgrInfo,
    _argument: Datum,
) {
    unimplemented!()
}
