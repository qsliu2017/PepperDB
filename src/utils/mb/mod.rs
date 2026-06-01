//! Multibyte string helpers living under utils/mb (postgres/src/backend/utils/mb).
//!
//! The bulk of the encoding machinery is in the top-level `crate::mb`
//! (mbutils/wchar); this holds the small wide-char string-compare helpers.

pub mod stringinfo_mb;
pub mod wstrcmp;
pub mod wstrncmp;
