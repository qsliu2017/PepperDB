//! Translated from PostgreSQL src/include/storage/item.h

// C: `typedef Pointer Item;` (Pointer is `char *`). An Item is an untyped
// pointer into a page buffer; modeled as a byte slice at the safe surface.
pub type Item<'a> = &'a [u8];
