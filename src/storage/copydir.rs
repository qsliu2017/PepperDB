//! Translated from PostgreSQL src/include/storage/copydir.h

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FileCopyMethod {
    Copy = 0,
    Clone = 1,
}

/// GUC parameter.
pub static mut FILE_COPY_METHOD: i32 = 0;

pub fn copydir(_fromdir: &str, _todir: &str, _recurse: bool) {
    unimplemented!()
}

pub fn copy_file(_fromfile: &str, _tofile: &str) {
    unimplemented!()
}
