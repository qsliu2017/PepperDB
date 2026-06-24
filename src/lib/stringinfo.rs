//! Tombstone: src/include/lib/stringinfo.h
//!
//! `StringInfoData` (an extensible string/byte buffer) is replaced by std types:
//! `String` for text and `Vec<u8>` for binary buffers (see translation-rules
//! container table). The read-cursor variant used by the wire protocol maps to
//! `std::io::Cursor<&[u8]>` (or a slice + index) at those call sites. Callers use
//! the std types directly; there is no `StringInfoData` type.
