//! Translated from PostgreSQL src/include/windowapi.h
//! API for window functions to extract data from their window.

use crate::postgres::Datum;

/// Values of "seektype".
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowSeek {
    Current = 0,
    Head = 1,
    Tail = 2,
}

/// Opaque; private in nodeWindowAgg.c. C: `typedef struct WindowObjectData *WindowObject;`
pub struct WindowObjectData;
pub type WindowObject = Box<WindowObjectData>; // TODO(ptr)

/// SAFETY/typing helper. C: `WinGetPartitionLocalMemory` returns `void *`; one
/// real element type per use site -> generic raw region modeled as a byte slice.
pub fn WinGetPartitionLocalMemory(_winobj: &mut WindowObjectData, _sz: usize) -> &mut [u8] {
    unimplemented!()
}

pub fn WinGetCurrentPosition(_winobj: &WindowObjectData) -> i64 {
    unimplemented!()
}
pub fn WinGetPartitionRowCount(_winobj: &WindowObjectData) -> i64 {
    unimplemented!()
}

pub fn WinSetMarkPosition(_winobj: &mut WindowObjectData, _markpos: i64) {
    unimplemented!()
}

pub fn WinRowsArePeers(_winobj: &mut WindowObjectData, _pos1: i64, _pos2: i64) -> bool {
    unimplemented!()
}

/// Result of fetching an argument: value plus is-null and is-outside-window flags.
pub struct WinArg {
    pub value: Datum,
    pub isnull: bool,
    pub isout: bool, // requested row fell outside the partition/frame
}

pub fn WinGetFuncArgInPartition(
    _winobj: &mut WindowObjectData,
    _argno: i32,
    _relpos: i32,
    _seektype: WindowSeek,
    _set_mark: bool,
) -> WinArg {
    unimplemented!()
}

pub fn WinGetFuncArgInFrame(
    _winobj: &mut WindowObjectData,
    _argno: i32,
    _relpos: i32,
    _seektype: WindowSeek,
    _set_mark: bool,
) -> WinArg {
    unimplemented!()
}

/// isnull out-param folded into the return (function-mapping 5.1).
pub fn WinGetFuncArgCurrent(_winobj: &mut WindowObjectData, _argno: i32) -> Option<Datum> {
    unimplemented!()
}
