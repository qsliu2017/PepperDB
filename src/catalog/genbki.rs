//! catalog/genbki.h - genbki macros for catalog header files (BKI bootstrap).
//!
//! genbki.h defines CATALOG(), BKI_BOOTSTRAP and related macros so that the
//! catalog header files can be read by the C compiler. These same words are
//! recognized by genbki.pl to build the BKI bootstrap file from these header
//! files.
//!
//! In C, every macro in this header expands either to nothing (a pure
//! annotation consumed only by genbki.pl) or to `extern int no_such_variable`
//! (a throwaway declaration to keep the C compiler quiet). The lone exception
//! is CATALOG(), which introduces a `typedef struct FormData_<name>`.
//!
//! In the Rust port these macros have NO runtime meaning: the catalog struct
//! definitions are written directly as Rust structs, and the BKI annotations
//! (BKI_DEFAULT, BKI_LOOKUP, DECLARE_INDEX, ...) carry no code. We materialize
//! them here as no-op `macro_rules!` so that any 1:1 translation of a catalog
//! header that still references these names will compile. They expand to
//! nothing, exactly mirroring the C preprocessor behavior.

// =========================================================================
// CATALOG() - introduces a catalog's structure definition.
//
// C: #define CATALOG(name,oid,oidmacro) typedef struct CppConcat(FormData_,name)
//
// In C this turns the `CATALOG(pg_foo,1234,FooRelation_Rowtype_Id) { ... };`
// line into `typedef struct FormData_pg_foo { ... } FormData_pg_foo;`. There
// is no faithful token-for-token Rust equivalent (Rust has no struct-prefix
// macro of this shape). In the Rust port the catalog headers define their
// FormData_<name> structs directly as `#[repr(C)] pub struct FormData_<name>`,
// so this macro is provided only as a documented no-op placeholder; it is not
// expected to be invoked.
// =========================================================================

/// No-op genbki.pl annotation macros: each accepts any tokens and expands to
/// nothing, matching the empty C `#define`s. Written out directly (a generator
/// macro would need the unstable `$$` meta-variable-expression escape).
#[macro_export]
macro_rules! BKI_BOOTSTRAP { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_SHARED_RELATION { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_ROWTYPE_OID { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_SCHEMA_MACRO { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_FORCE_NULL { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_FORCE_NOT_NULL { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_DEFAULT { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_ARRAY_DEFAULT { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_LOOKUP { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! BKI_LOOKUP_OPT { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_TOAST { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_TOAST_WITH_MACRO { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_INDEX { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_UNIQUE_INDEX { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_UNIQUE_INDEX_PKEY { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_OID_DEFINING_MACRO { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_FOREIGN_KEY { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_FOREIGN_KEY_OPT { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_ARRAY_FOREIGN_KEY { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! DECLARE_ARRAY_FOREIGN_KEY_OPT { ($($tt:tt)*) => {}; }
#[macro_export]
macro_rules! MAKE_SYSCACHE { ($($tt:tt)*) => {}; }

/*
 * The following C symbols are "never defined; they are here only for
 * documentation":
 *
 *   #undef CATALOG_VARLEN
 *       Variable-length catalog fields are made invisible to C structures by
 *       `#ifdef CATALOG_VARLEN`. Since the symbol is undefined, those fields
 *       are excluded from the in-memory FormData struct. In the Rust port the
 *       corresponding fields are simply omitted from the FormData_<name>
 *       struct definitions, so there is no Rust symbol to emit here.
 *
 *   #undef EXPOSE_TO_CLIENT_CODE
 *       Marks header sections (via `#ifdef EXPOSE_TO_CLIENT_CODE`) that
 *       genbki.pl copies into the generated "_d" header for client code. Not
 *       a compile-time C symbol; nothing to emit in Rust.
 *
 * These are documented here for completeness; they intentionally produce no
 * Rust definitions.
 */
