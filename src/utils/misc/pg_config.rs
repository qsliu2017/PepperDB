//! pg_config() SQL function - expose configure/build settings as a result set
//! (utils/misc/pg_config.c). 1:1 translation.

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use core::ffi::c_char;

// ---- dependency stubs (TODO(pg-port): wire to real homes) ----

/// ReturnSetInfo - set-returning function support (nodes/execnodes.h).
#[repr(C)]
pub struct ReturnSetInfo {
    pub resultinfo_header: [u8; 0],
    pub setResult: *mut core::ffi::c_void, // Tuplestorestate *
    pub setDesc: *mut core::ffi::c_void,   // TupleDesc
}

/// ConfigData - one name/setting pair (common/config_info.h).
#[repr(C)]
pub struct ConfigData {
    pub name: *mut c_char,
    pub setting: *mut c_char,
}

// my_exec_path: the running executable's path (miscadmin / globals).
extern "C" {
    pub static mut my_exec_path: [c_char; 1024];
}

unsafe fn get_configdata(_my_exec_path: *const c_char, configdata_len: *mut usize) -> *mut ConfigData {
    // TODO(pg-port): common/config_info.c get_configdata()
    *configdata_len = 0;
    core::ptr::null_mut()
}

unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: i32) {
    // TODO(pg-port): funcapi.c InitMaterializedSRF()
}

unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    // TODO(pg-port): utils/builtins.h CStringGetTextDatum()
    0
}

unsafe fn tuplestore_putvalues(
    _state: *mut core::ffi::c_void,
    _tdesc: *mut core::ffi::c_void,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    // TODO(pg-port): utils/sort/tuplestore.c tuplestore_putvalues()
}

/*
 * pg_config: supply a list of configuration settings
 */
pub unsafe fn pg_config(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let configdata: *mut ConfigData;
    let mut configdata_len: usize = 0;
    let mut i: usize = 0;

    /* initialize our tuplestore */
    InitMaterializedSRF(fcinfo, 0);

    configdata = get_configdata(my_exec_path.as_ptr(), &mut configdata_len);
    while i < configdata_len {
        let mut values: [Datum; 2] = [0; 2];
        let mut nulls: [bool; 2] = [false; 2];

        values[0] = CStringGetTextDatum((*configdata.add(i)).name);
        values[1] = CStringGetTextDatum((*configdata.add(i)).setting);

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
        i += 1;
    }

    0 as Datum
}
