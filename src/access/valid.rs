//! Translated from PostgreSQL src/include/access/valid.h

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::heap_getattr;
use crate::access::skey::{ScanKeyData, ScanKeyFlags};
use crate::access::tupdesc::TupleDesc;
use crate::fmgr::FunctionCall2Coll;
use crate::postgres::DatumGetBool;

/// Test a heap tuple to see if it satisfies a scan key.
pub fn HeapKeyTest(tuple: &HeapTupleData, tupdesc: &TupleDesc, keys: &mut [ScanKeyData]) -> bool {
    for cur_key in keys.iter_mut() {
        if cur_key.flags & ScanKeyFlags::ISNULL.bits() != 0 {
            return false;
        }

        let (atp, isnull) = heap_getattr(tuple, i32::from(cur_key.attno), tupdesc);
        if isnull {
            return false;
        }

        let test = FunctionCall2Coll(
            &mut cur_key.func,
            cur_key.collation,
            atp,
            cur_key.argument,
        );

        match test {
            Some(d) if DatumGetBool(d) => {}
            _ => return false,
        }
    }

    true
}
