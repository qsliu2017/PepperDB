//! Translated from PostgreSQL src/include/snowball/header.h
//
// TOMBSTONE: build shim for the Snowball stemmer C sources. It only forces
// `postgres.h` to be included first and redefines malloc/calloc/realloc/free to
// palloc/pfree so the generated stemmer .c files allocate in a MemoryContext.
// In Rust there are no generated C stemmer files and no palloc macros to
// override, so nothing carries over. A Rust stemmer would use ordinary
// allocation / a crate. No items to translate.
