//! Translated from PostgreSQL src/include/snowball/libstemmer/header.h
//
// Tombstone: vendored Snowball stemmer runtime. Replaced wholesale by the
// `rust-stemmers` crate; the Snowball `SN_env` machinery (create_s, find_among,
// slice_*, in_grouping, eq_s, etc.) is not ported.
