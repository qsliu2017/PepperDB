//! File access support (postgres/src/backend/storage/file).
//!
//! So far: shared file sets (`fileset`) and directory copy (`copydir`). The
//! virtual file descriptor layer (fd.c) is future work.

pub mod copydir;
pub mod fileset;
pub mod reinit;

pub mod sharedfileset;
pub mod fd;
pub mod buffile;
