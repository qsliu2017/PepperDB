//! Translated from PostgreSQL src/include/postgres.h
//!
//! NOTE: postgres.h sits at topological level 4, but `Datum`/`NullableDatum` are
//! ambient foundational types that earlier-level headers (fmgr.h, varatt.h, ...)
//! use without an #include. They are seeded here per translation-rules.md
//! (`Datum` = newtype over `usize`, pointer-width). The level-4 pass extends this
//! module; it must not redefine these.

/// `Datum` = `uintptr_t`: a pointer-width tagged value. NOT `u64` - it aliases
/// pointers. Per-type interpretation is up to the caller (see fmgr `DatumGetX`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct Datum(pub usize);

impl Datum {
    pub const fn from_bool(b: bool) -> Self {
        Datum(b as usize)
    }
    pub const fn get_bool(self) -> bool {
        self.0 != 0
    }
}

/// A `Datum` paired with its nullness, for places that need both together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NullableDatum {
    pub value: Datum,
    pub isnull: bool,
}
