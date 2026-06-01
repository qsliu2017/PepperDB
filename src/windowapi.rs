//! windowapi.h - API for window functions to extract data from their window.
//!
//! A window function receives a "WindowObject" (via PG_WINDOW_OBJECT()) which
//! it uses to query the current row number, partition row count, and to
//! evaluate its argument expressions at various rows in the window partition.

use std::ffi::{c_int, c_void};

use crate::c::{int64, Size};
use crate::nodes::execnodes::WindowObjectData;
use crate::postgres::Datum;
use crate::utils::fmgr::FunctionCallInfo;

/* values of "seektype" */
pub const WINDOW_SEEK_CURRENT: c_int = 0;
pub const WINDOW_SEEK_HEAD: c_int = 1;
pub const WINDOW_SEEK_TAIL: c_int = 2;

/* this struct is private in nodeWindowAgg.c */
pub type WindowObject = *mut WindowObjectData;

/// `#define PG_WINDOW_OBJECT() ((WindowObject) fcinfo->context)`
///
/// In C this macro implicitly reads an `fcinfo` from the enclosing scope; here
/// the FunctionCallInfo is passed explicitly. `context` is an `fmNodePtr`
/// (`*mut Node`).
#[inline]
pub unsafe fn PG_WINDOW_OBJECT(fcinfo: FunctionCallInfo) -> WindowObject {
    (*fcinfo).context as WindowObject
}

/// `#define WindowObjectIsValid(winobj) \`
/// `    ((winobj) != NULL && IsA(winobj, WindowObjectData))`
///
/// TODO(pg-port): NodeTag has no `T_WindowObjectData` variant yet (adding it
/// mid-enum would shift discriminants). The C macro also does `IsA(winobj,
/// WindowObjectData)`; until the tag exists this is the non-null check only.
#[inline]
pub unsafe fn WindowObjectIsValid(winobj: WindowObject) -> bool {
    !winobj.is_null()
}

pub unsafe fn WinGetPartitionLocalMemory(winobj: WindowObject, sz: Size) -> *mut c_void {
    unimplemented!()
}

pub unsafe fn WinGetCurrentPosition(winobj: WindowObject) -> int64 {
    unimplemented!()
}

pub unsafe fn WinGetPartitionRowCount(winobj: WindowObject) -> int64 {
    unimplemented!()
}

pub unsafe fn WinSetMarkPosition(winobj: WindowObject, markpos: int64) {
    unimplemented!()
}

pub unsafe fn WinRowsArePeers(winobj: WindowObject, pos1: int64, pos2: int64) -> bool {
    unimplemented!()
}

pub unsafe fn WinGetFuncArgInPartition(
    winobj: WindowObject,
    argno: c_int,
    relpos: c_int,
    seektype: c_int,
    set_mark: bool,
    isnull: *mut bool,
    isout: *mut bool,
) -> Datum {
    unimplemented!()
}

pub unsafe fn WinGetFuncArgInFrame(
    winobj: WindowObject,
    argno: c_int,
    relpos: c_int,
    seektype: c_int,
    set_mark: bool,
    isnull: *mut bool,
    isout: *mut bool,
) -> Datum {
    unimplemented!()
}

pub unsafe fn WinGetFuncArgCurrent(
    winobj: WindowObject,
    argno: c_int,
    isnull: *mut bool,
) -> Datum {
    unimplemented!()
}
