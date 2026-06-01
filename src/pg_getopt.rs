//! pg_getopt.h - getopt(3) declarations for Postgres files that use getopt.

use std::ffi::{c_char, c_int};

// extern variables declared by the platform (or by us when <getopt.h> is
// absent). PGDLLIMPORT has no Rust analog; these are plain extern globals.
extern "C" {
    pub static mut optarg: *mut c_char;
    pub static mut optind: c_int;
    pub static mut opterr: c_int;
    pub static mut optopt: c_int;

    // Some platforms have optreset (HAVE_INT_OPTRESET && !__CYGWIN__).
    pub static mut optreset: c_int;
}

extern "C" {
    // Provided when the platform lacks getopt() (#ifndef HAVE_GETOPT).
    pub fn getopt(nargc: c_int, nargv: *const *mut c_char, ostr: *const c_char) -> c_int;
}
