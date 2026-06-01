//! Text-search subsystem (postgres/src/backend/tsearch + postgres/src/include/tsearch).
//!
//! So far: the locale/character-classification helpers (`ts_locale`). The
//! dictionaries, parsers, and configuration machinery are future work.

pub mod ts_typanalyze;
pub mod dict;
pub mod dict_ispell;
pub mod dict_simple;
pub mod dict_synonym;
pub mod regis;
pub mod ts_locale;
pub mod ts_selfuncs;
pub mod ts_public;
pub mod ts_utils;
pub mod ts_parse;
pub mod wparser;
pub mod dict_thesaurus;
