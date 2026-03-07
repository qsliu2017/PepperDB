//! Write-Ahead Log (WAL) -- sequential log of database changes for durability.
//! WAL records are written to 16MB segment files in `pg_wal/`, with page headers
//! at 8KB boundaries matching PostgreSQL's XLogPageHeaderData format.

pub mod record;
pub mod recovery;
pub mod writer;
