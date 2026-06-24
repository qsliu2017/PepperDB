//! Translated from PostgreSQL src/include/getopt_long.h
//
// Long-option parsing. PG's eventual replacement is `clap`; this preserves the
// C `struct option` and getopt_long signature for the skeleton.

/// has_arg values.
pub const no_argument: i32 = 0;
pub const required_argument: i32 = 1;
pub const optional_argument: i32 = 2;

/// C: `struct option`.
pub struct GetoptOption {
    pub name: &'static str,
    pub has_arg: i32,
    pub flag: Option<*mut i32>, // TODO(ptr): out-flag target
    pub val: i32,
}

/// C: `int getopt_long(argc, argv, optstring, longopts, int *longindex)`.
/// Returns the option char/val, or None at end of options.
pub fn getopt_long(
    args: &[String],
    optstring: &str,
    longopts: &[GetoptOption],
) -> Option<(i32, Option<usize>)> {
    // returns (val, matched longopt index)
    unimplemented!()
}
