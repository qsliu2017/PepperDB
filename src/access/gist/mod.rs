//! Generalized Search Tree access method (postgres/src/backend/access/gist).
//!
//! Header-only internal type layer so far: the shared private definitions
//! (`gist_private`). The GiST opclasses and index machinery are future work.

pub mod gist_private;
pub mod gistutil;
pub mod gistscan;
pub mod gistvalidate;
pub mod gistxlog;
pub mod gistvacuum;
pub mod gistbuildbuffers;
pub mod gistsplit;
pub mod gistget;
