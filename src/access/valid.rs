//! access/valid.h - POSTGRES tuple qualification validity definitions.

use std::ffi::c_int;

use crate::access::common::scankey::{ScanKey, SK_ISNULL};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::{heap_getattr, HeapTuple};
use crate::postgres::DatumGetBool;
use crate::utils::fmgr::FunctionCall2Coll;

/// HeapKeyTest
///
/// Test a heap tuple to see if it satisfies a scan key.
#[inline]
pub unsafe fn HeapKeyTest(
    tuple: HeapTuple,
    tupdesc: TupleDesc,
    nkeys: c_int,
    keys: ScanKey,
) -> bool {
    let mut cur_nkeys = nkeys;
    let mut cur_key = keys;

    while {
        let cont = cur_nkeys != 0;
        cur_nkeys -= 1;
        cont
    } {
        let mut isnull: bool = false;

        if (*cur_key).sk_flags & SK_ISNULL != 0 {
            return false;
        }

        let atp = heap_getattr(
            tuple,
            (*cur_key).sk_attno as c_int,
            tupdesc,
            &mut isnull,
        );

        if isnull {
            return false;
        }

        let test = FunctionCall2Coll(
            &mut (*cur_key).sk_func,
            (*cur_key).sk_collation,
            atp,
            (*cur_key).sk_argument,
        );

        if !DatumGetBool(test) {
            return false;
        }

        cur_key = cur_key.add(1);
    }

    true
}
