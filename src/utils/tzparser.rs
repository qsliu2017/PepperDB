//! Translated from PostgreSQL src/include/utils/tzparser.h

use crate::utils::datetime::TimeZoneAbbrevTable;

/// The result of parsing a timezone configuration file is an array of these
/// structs, in order by abbrev. In-memory.
pub struct TzEntry {
    /// TZ abbreviation (downcased)
    pub abbrev: String,
    /// zone name if dynamic abbrev, else None
    pub zone: Option<String>,
    /// offset in seconds from UTC (unused for dynamic abbrev)
    pub offset: i32,
    /// true if a DST abbreviation (unused for dynamic abbrev)
    pub is_dst: bool,
    /// source line number (for error messages)
    pub lineno: i32,
    /// source filename (for error messages)
    pub filename: String,
}

pub fn load_tzoffsets(_filename: &str) -> TimeZoneAbbrevTable {
    unimplemented!()
}
