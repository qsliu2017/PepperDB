//! Translated from PostgreSQL src/include/access/gistscan.h

use crate::access::genam::IndexScanDesc;
use crate::access::skey::ScanKey;
use crate::utils::rel::Relation;

pub fn gistbeginscan(_r: Relation, _nkeys: i32, _norderbys: i32) -> IndexScanDesc {
    unimplemented!()
}
pub fn gistrescan(
    _scan: IndexScanDesc,
    _key: ScanKey,
    _nkeys: i32,
    _orderbys: ScanKey,
    _norderbys: i32,
) {
    unimplemented!()
}
pub fn gistendscan(_scan: IndexScanDesc) {
    unimplemented!()
}
