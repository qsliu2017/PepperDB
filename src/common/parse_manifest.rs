//! Translated from PostgreSQL src/include/common/parse_manifest.h
//! Parse a backup manifest in JSON format.

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::common::checksum_helper::pg_checksum_type;

/// Opaque incremental-parse state.
pub struct JsonManifestParseIncrementalState;

/// The C `JsonManifestParseContext` is a `void *private_data` plus a table of
/// callbacks (routine-struct / opaque-context idiom). It maps to a trait the
/// caller implements; `private_data` becomes `self`.
pub trait JsonManifestParseContext {
    fn version(&mut self, manifest_version: i32);
    fn system_identifier(&mut self, manifest_system_identifier: u64);
    fn per_file(
        &mut self,
        pathname: &str,
        size: u64,
        checksum_type: pg_checksum_type,
        checksum_payload: &[u8],
    );
    fn per_wal_range(&mut self, tli: TimeLineID, start_lsn: XLogRecPtr, end_lsn: XLogRecPtr);
    /// Error reporter; does not return in C. `// TODO(panic)`.
    fn error(&mut self, msg: &str) -> !;
}

pub fn json_parse_manifest(_context: &mut dyn JsonManifestParseContext, _buffer: &[u8]) {
    unimplemented!()
}

pub fn json_parse_manifest_incremental_init(
    _context: &mut dyn JsonManifestParseContext,
) -> JsonManifestParseIncrementalState {
    unimplemented!()
}

pub fn json_parse_manifest_incremental_chunk(
    _incstate: &mut JsonManifestParseIncrementalState,
    _chunk: &[u8],
    _is_last: bool,
) {
    unimplemented!()
}

pub fn json_parse_manifest_incremental_shutdown(_incstate: JsonManifestParseIncrementalState) {
    unimplemented!()
}
